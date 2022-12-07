from pg_data_etl import Database
import geopandas as gpd
import os
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

db = Database.from_config("mercer", "omad")
gis_db = Database.from_config("gis", "gis")
conflate_db = Database.from_config("conflate", "conflate")
data_folder = Path(os.getenv("data_root"))  # path to g drive folder'


def conflation_schema():
    query = """drop schema if exists tmp CASCADE;
                create schema tmp;
                drop schema if exists conflated CASCADE;
                create schema conflated;"""
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
    query = f"""create table tmp.{output_table}_to_centerline as
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
    distance_threshold: int = 5,
):
    conflation_schema()
    convert_to_point(input_table, output_table, unique_id)
    point_to_base_layer(base_layer, output_table, distance_threshold)
    point_count(output_table, distance_threshold)
    total_point_count(output_table)
    most_occuring_in_threshold(output_table, distance_threshold)
    conflate_to_base(output_table, distance_threshold, base_layer)

    pass


if __name__ == "__main__":
    conflator("view_pm_vc100", "pmvc100", "uid", "nj_centerline", 5)
