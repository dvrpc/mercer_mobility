from pg_data_etl import Database
import os
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

db = Database.from_config("mercer", "omad")
gis_db = Database.from_config("gis", "gis")
data_folder = Path(os.getenv("data_root"))  # path to g drive folder'


def conflation_schema():
    query = """drop schema if exists tmp CASCADE;
                create schema tmp;
                drop schema if exists conflated CASCADE;
                create schema conflated;
                drop schema if exists rejoined CASCADE;
                create schema rejoined;"""
    db.execute(query)


def convert_to_point(input_table: str, output_table: str, unique_id: str):
    # converts line layer to be conflated to point using st_interpolate point
    query = f"""drop table if exists tmp.{output_table}_pt;
                create table tmp.{output_table}_pt as
                select
                    n,
                    {unique_id} as id,
                    ST_LineInterpolatePoint(
                        st_linemerge((st_dump(geom)).geom),
                    least(n *(4 / st_length(geom)), 1.0)
                    )::GEOMETRY(POINT,
                    26918) as geom
                from
                    {input_table}
                cross join Generate_Series(0, ceil(st_length(geom) / 4)::INT) as n
                where
                    st_length(geom) > 0;"""
    db.execute(query)


def point_to_base_layer(baselayer: str, output_table: str, distance_threshold: int):
    # selects points that are within threshold distance from base layer (i.e. layer you're conflating to)
    query = f"""drop table if exists tmp.{output_table}_point_to_base;
                create table tmp.{output_table}_point_to_base as
                select
                    a.id as {output_table}_id,
                    b.globalid,
                    a.geom
                from
                    tmp.{output_table}_pt a,
                    public.{baselayer} b
                where
                    st_dwithin(a.geom,
                    b.geom,
                    {distance_threshold})
                order by
                    a.n,
                    st_distance(a.geom,
                    b.geom);
                """
    db.execute(query)


def point_count(output_table: str, distance_threshold: int):
    # counts the number of records that are within distance threshold of base layer
    query = f"""drop table if exists tmp.{output_table}_point{distance_threshold}_count;
            create table tmp.{output_table}_point{distance_threshold}_count as
            select
                globalid,
                count(*) as pnt_{distance_threshold}_count,
                {output_table}_id
            from
                tmp.{output_table}_point_to_base
            group by
                globalid,
                {output_table}_id;
            """
    db.execute(query)


def total_point_count(output_table: str):
    # counts total points in line layer
    query = f"""drop table if exists tmp.{output_table}_total_point_count;
                create table tmp.{output_table}_total_point_count as
                select
                    globalid,
                    count(*) as {output_table}_total_point_count
                from
                    tmp.{output_table}_point_to_base
                group by
                    globalid;
                            """
    db.execute(query)


def most_occuring_in_threshold(output_table: str, distance_threshold: int):
    # finds percent match of points within distance threshold vs total points
    query = f"""drop table if exists tmp.{output_table}point{distance_threshold}_most_occurring;
                create table tmp.{output_table}point{distance_threshold}_most_occurring as
                select
                    distinct on
                    (a.globalid) a.globalid,
                    a.pnt_{distance_threshold}_count,
                    b.{output_table}_total_point_count,
                    round(
                        (
                            (
                                a.pnt_{distance_threshold}_count::numeric / b.{output_table}_total_point_count::numeric
                            ) * 100
                        ),
                        0
                    ) as pnt_{distance_threshold}_pct_match,
                    a.{output_table}_id
                from
                    tmp.{output_table}_point{distance_threshold}_count a
                left join tmp.{output_table}_total_point_count b on
                    (a.globalid = b.globalid)
                order by
                    a.globalid,
                    a.pnt_{distance_threshold}_count desc;
                            """
    db.execute(query)


def conflate_to_base(output_table: str, distance_threshold: int, baselayer: str):
    # finds percent match of points within distance threshold vs total points
    query = f"""drop table if exists conflated.{output_table}_to_{baselayer};
                create table conflated.{output_table}_to_{baselayer} as
                select
                    distinct on
                    (a.globalid) a.*,
                    b.{output_table}_id,
                    b.pnt_{distance_threshold}_count,
                    b.{output_table}_total_point_count,
                    b.pnt_{distance_threshold}_pct_match,
                    round(
                        (st_length(a.geom) / 4)::numeric,
                        0
                    ) as total_possible_pnts,
                    round(
                        (
                            (
                                b.{output_table}_total_point_count / (st_length(a.geom) / 4)
                            ) * 100
                        )::numeric,
                        0
                    ) as possible_coverage
                from
                    {baselayer} a
                left join tmp.{output_table}point{distance_threshold}_most_occurring b on
                    a.globalid = b.globalid;
                            """
    db.execute(query)


def conflator(
    input_table: str,
    output_table: str,
    unique_id: str,
    base_layer: str,
    column: str,
    distance_threshold: int = 5,
    coverage_threshold: int = 70,
):
    convert_to_point(input_table, output_table, unique_id)
    point_to_base_layer(base_layer, output_table, distance_threshold)
    point_count(output_table, distance_threshold)
    total_point_count(output_table)
    most_occuring_in_threshold(output_table, distance_threshold)
    conflate_to_base(output_table, distance_threshold, base_layer)

    # necessary to rejoin the conflated geometry back to the id of the original geometry. might be a better way to do this
    query = f"""
        drop table if exists rejoined.{output_table};
        create table rejoined.{output_table} as
            select
                a.*,
                {column}
            from conflated.{output_table}_to_nj_centerline a
            inner join public.{input_table} b
            on a.{output_table}_id = b.uid
            where a.possible_coverage > {coverage_threshold}"""
    db.execute(query)


def rejoiner():
    query = """
        drop table if exists rejoined.all;
        create table rejoined.all as
        select 
            a."index", 
            a.sri, 
            a.geom, 
            b."volcapra~2" as amvc100, 
            c."volcapra~2" as amvc85, 
            d.crrate, 
            e.bikefacili, 
            f."type" as countyrd, 
            g.line, 
            h.pci_new, 
            i."volcapra~2" as pmvc100,
            j."volcapra~2" as pmvc85,
            k.ptiwkd0708,
            k.ptiwkd0809,
            k.ptiwkd1617,
            k.ptiwkd1718,
            l.ttiwkd0708,
            l.ttiwkd0809,
            l.ttiwkd1617,
            l.ttiwkd1718,
            m.sw_ratio,
            o."countact~2" as busfreq,
            o."r_counta~5" as busfreq2
        from public.nj_centerline a
        left join rejoined.amvc100 b 
            on b."index" = a."index" 
        left join rejoined.amvc85 c 
            on c."index" = a."index" 
        left join rejoined.crash_seg d
            on d."index" = a."index" 
        left join rejoined.lts_no_facils e 
            on e."index" = a."index" 
        left join rejoined.mercer_roads f
            on f."index" = a."index" 
        left join rejoined.njt g
            on g."index" = a."index" 
        left join rejoined.pavement h
            on h."index" = a."index" 
        left join rejoined.pmvc100 i
            on i."index" = a."index" 
        left join rejoined.pmvc85 j
            on j."index" = a."index" 
        left join rejoined.pti k
            on k."index" = a."index" 
        left join rejoined.tti l
            on l."index" = a."index" 
        left join rejoined.sidewalk_gaps m
            on m."index" = a."index" 
        left join rejoined.bus_freq o
            on o."index" = a."index"
    """
    db.execute(query)


if __name__ == "__main__":
    conflation_schema()

    # model outputs, possible coverage >= 70
    conflator("view_am_vc100", "amvc100", "uid", "nj_centerline", 'b."volcapra~2"')
    conflator("view_pm_vc100", "pmvc100", "uid", "nj_centerline", 'b."volcapra~2"')
    conflator("view_am_vc85", "amvc85", "uid", "nj_centerline", 'b."volcapra~2"')
    conflator("view_pm_vc85", "pmvc85", "uid", "nj_centerline", 'b."volcapra~2"')

    # pti/tti, possible coverage >= 80
    for i in ["tti", "pti"]:
        conflator(
            f"view_{i}_all",
            f"{i}",
            "uid",
            "nj_centerline",
            "b.ttiwkd0708,b.ttiwkd0809,b.ttiwkd1617,b.ttiwkd1718, b.ptiwkd0708,b.ptiwkd0809, b.ptiwkd1617,b.ptiwkd1718",
            10,
            80,
        )

    # nj_transit routes, possible coverage >=80
    conflator("nj_transit_routes", "njt", "uid", "nj_centerline", "b.line", 8, 80)

    # mercer jurisdiction roads, possible coverage >= 75
    conflator(
        "mercercountyjurisdictionroads_frommercer",
        "mercer_roads",
        "uid",
        "nj_centerline",
        "b.type",
        8,
        75,
    )

    # pavement condition, possible coverage >= 75
    conflator(
        "pavement_evaluation", "pavement", "uid", "nj_centerline", "b.pci_new", 5, 75
    )

    # crash segments, possible coverage >= 75
    conflator(
        "crash_statistics_by_segment_mc",
        "crash_seg",
        "uid",
        "nj_centerline",
        "b.crrate",
        5,
        75,
    )

    # sw gaps (single segment in center of street) possible coverage >= 75
    conflator(
        "sidewalk_gaps_clipped",
        "sidewalk_gaps",
        "uid",
        "nj_centerline",
        "b.sw_ratio",
        5,
        75,
    )

    # bike facilities (layer tbd)
    conflator(
        "lts_deficient_facils",
        "lts_no_facils",
        "uid",
        "nj_centerline",
        "b.bikefacili",
        5,
    )

    conflator(
        "bus_frequency_link",
        "bus_freq",
        "uid",
        "nj_centerline",
        'b."countact~2", b."r_counta~5"',
        5,
        75,
    )
    rejoiner()
