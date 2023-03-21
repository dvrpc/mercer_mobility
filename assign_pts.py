from pg_data_etl import Database
from dotenv import load_dotenv
import os

load_dotenv()
api_key = os.getenv("api_key")

db = Database.from_config("mercer", "omad")


def megajoin():
    query = """
    drop schema if exists point_assignment CASCADE;
    create schema point_assignment;
    create table point_assignment.megajoin as (
        select 
            a.*, 
            b.inrixxd, 
            b.ampmdelay as bottleneckvehdelay,
            c.lsad_type
        from rejoined.all a
            left join public.bottlenecksvehvoldelay b
                on st_within(b.geom, st_buffer(a.geom, 10))
            left join public.uza c 
                on st_within(a.geom, c.geom));
    """
    print("creating base scenario")
    db.execute(query)


def create_point_cols():
    deficiencies = [
        "vulusercrrate",
        "ksicrrate",
        "crrate",
        "sidewalk",
        "missing_bike_fac",
        "transit_rt",
        "tti",
        "pti",
        "bottleneck",
        "vehvoldelay",
    ]
    col_names = []
    for value in deficiencies:
        v2 = value + "_pts"
        col_names.append(v2)

    for column in col_names:
        query = f"""
            alter table point_assignment.megajoin drop column if exists {column};
            alter table point_assignment.megajoin add column {column} integer;
            update point_assignment.megajoin set {column} = 0 where {column} is null;"""
        db.execute(query)


def assign_points(table: str, point_col: str, point: int, where_statement: str):
    query = f"""
    UPDATE point_assignment.{table} SET {point_col} = {point} WHERE {where_statement}
    """
    db.execute(query)


def total_points(table: str):
    query = f"""
    alter table point_assignment.{table} add column if not exists total int;
    UPDATE point_assignment.{table} set total = vulusercrrate_pts + vc_pts + ksicrrate_pts + crrate_pts + sidewalk_pts + missing_bike_fac_pts + tti_pts + pti_pts + bottleneck_pts + transit_rt_pts + vehvoldelay_pts;
    """
    db.execute(query)


def avg_and_sd(column: str, table: str):
    """returns average and sd, calculated in pg"""
    avg = db.query_as_singleton(f"select avg({column}) from point_assignment.{table}")
    sd = db.query_as_singleton(f"select stddev({column}) from point_assignment.{table}")
    return [avg, sd]


if __name__ == "__main__":
    megajoin()
    create_point_cols()
