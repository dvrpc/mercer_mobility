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
    query = """drop schema if exists tmp;
                create schema tmp;
                drop schema if exists conflated;
                create schema conflated;"""
    db.execute(query)


def convert_to_point():
    # converts line layer to be conflated to point using st_interpolate point
    query = """drop table if exists tmp.vc100_pt;
                create table tmp.vc100_pt as
                select
                    n,
                    "no" as id,
                    ST_LineInterpolatePoint(
                        st_linemerge((st_dump(geom)).geom),
                    least(n *(4 / st_length(geom)), 1.0)
                    )::GEOMETRY(POINT,
                    26918) as geom
                from
                    view_am_vc100
                cross join Generate_Series(0, ceil(st_length(geom) / 4)::INT) as n
                where
                    st_length(geom) > 0;"""
    db.execute(query)


def point_to_base_layer():
    # selects points that are within threshold distance from base layer (i.e. layer you're conflating to)
    query = """create view tmp_vc100_point_to_xd as
                select
                    a.id as vc100_id,
                    b.globalid,
                    a.geom
                from
                    vc100_pt a,
                    nj_centerline b
                where
                    st_dwithin(a.geom,
                    b.geom,
                    5)
                order by
                    a.n,
                    st_distance(a.geom,
                    b.geom);
                """
    db.execute(query)


def point_count():
    # counts the number of records that are within distance threshold of base layer
    query = """create view tmp_vc100_point10_count as
            select
                globalid,
                count(*) as pnt_10_count,
                vc100_id
            from
                tmp_vc100_point_to_xd
            group by
                globalid,
                vc100_id;
            """
    db.execute(query)


def total_point_count():
    # counts total points in line layer
    query = """create view tmp_vc100_total_point_count as
                select
                    globalid,
                    count(*) as vc100_total_point_count
                from
                    tmp_vc100_point_to_xd
                group by
                    globalid;
                            """
    db.execute(query)


def most_occuring_in_threshold():
    # finds percent match of points within distance threshold vs total points
    query = """create view tmp_vc100_point10_most_occurring as
                select
                    distinct on
                    (a.globalid) a.globalid,
                    a.pnt_10_count,
                    b.vc100_total_point_count,
                    round(
                        (
                            (
                                a.pnt_10_count::numeric / b.vc100_total_point_count::numeric
                            ) * 100
                        ),
                        0
                    ) as pnt_10_pct_match,
                    a.vc100_id
                from
                    tmp_vc100_point10_count a
                left join tmp_vc100_total_point_count b on
                    (a.globalid = b.globalid)
                order by
                    a.globalid,
                    a.pnt_10_count desc;
                            """
    db.execute(query)


def conflate_to_base():
    # finds percent match of points within distance threshold vs total points
    query = """create view tmp_vc100_to_centerline as
                select
                    distinct on
                    (a.globalid) a.*,
                    b.vc100_id,
                    b.pnt_10_count,
                    b.vc100_total_point_count,
                    b.pnt_10_pct_match,
                    round(
                        (st_length(a.geom) / 4)::numeric,
                        0
                    ) as total_possible_pnts,
                    round(
                        (
                            (
                                b.vc100_total_point_count / (st_length(a.geom) / 4)
                            ) * 100
                        )::numeric,
                        0
                    ) as possible_coverage
                from
                    nj_centerline a
                left join tmp_vc100_point10_most_occurring b on
                    a.globalid = b.globalid;
                            """
    db.execute(query)


if __name__ == "__main__":
    # conflation_schema()
    convert_to_point()
    # point_to_base_layer()
    # point_count()
    # total_point_count()
    # most_occuring_in_threshold()
    # conflate_to_base()
