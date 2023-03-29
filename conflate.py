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
    print(f"conflating {input_table} to {base_layer}")
    db.execute(query)


def rejoiner():
    query = """
        drop table if exists rejoined.all;
        create table rejoined.all as
        select 
            a."index", 
            a.sri, 
            a.geom, 
            b.crrate, 
            b.ksicrrate,
            b.vulcrrate,
            c.bikefacili, 
            d."type" as countyrd, 
            e.line, 
            f.sw_ratio,
            g."countact~2" as busfreq,
            g."r_counta~5" as busfreq2,
            h.rankvehdel,
            h.rankvoldel
        from public.nj_centerline a
        left join rejoined.crash_seg b
            on b."index" = a."index" 
        left join rejoined.lts_ c 
            on c."index" = a."index" 
        left join rejoined.mercer_roads d
            on d."index" = a."index" 
        left join rejoined.njt e
            on e."index" = a."index" 
        left join rejoined.sidewalk_gaps f 
            on f."index" = a."index" 
        left join rejoined.bus_freq g 
            on g."index" = a."index"
        left join rejoined.bottlenecks h 
            on h."index" = a."index"
    """
    db.execute(query)


if __name__ == "__main__":
    conflation_schema()

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

    # crash segments, possible coverage >= 75
    conflator(
        "crash_statistics_by_segment_mc",
        "crash_seg",
        "uid",
        "nj_centerline",
        "b.crrate, b.ksicrrate, ((b.pedestrian + b.pedalcycli)*100000000)/(365*5*(b.endmilepo-b.startmilep)*b.average_da) as vulcrrate",
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

    # bike facilities
    conflator(
        "lts",
        "lts_",
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

    conflator(
        "bottlenecksegmentswithdelayv1",
        "bottlenecks",
        "uid",
        "nj_centerline",
        "b.cnbtv3 as bneck_id, b.roadname, b.rankvehdel, b.rankvoldel",
    )

    rejoiner()
