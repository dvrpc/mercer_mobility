from pg_data_etl import Database
import os
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

db = Database.from_config("mercer", "omad")
gis_db = Database.from_config("gis", "gis")
data_folder = Path(os.getenv("data_root"))  # path to g drive folder'

scenarios = ['a', 'b1', 'b2', 'c', 'd', 'e']

def megajoin():
    query = """
    drop schema if exists point_assignment CASCADE;
    create schema point_assignment;
    create table point_assignment.megajoin as (
        with 
        vul_crashes as(
            select a."index" , count(b.geom) as vul_crash from rejoined."all" a 
            inner join public.view_vulnerable_user_crashes b
            on st_within(b.geom, st_buffer(a.geom, 10))
            group by a.index),
        ksi as (
            select a."index" , count(b.geom) as ksi from rejoined."all" a 
            inner join public.view_ksi b
            on st_within(b.geom, st_buffer(a.geom, 10))
            group by a.index)
        select 
            a.*, 
            b.inrixxd, 
            c.unofficial_sufficiency_rating as bridge_rating,
            d.vul_crash,
            e.ksi,
            f.lsad_type
        from rejoined.all a
            left join public.bottlenecks b
                on st_within(b.geom, st_buffer(a.geom, 10))
            left join public.bridges_joined c
                on st_within(c.geom, st_buffer(a.geom, 10))
            left join vul_crashes d
                on a."index" = d."index" 
            left join ksi e
                on a."index" = e."index"
            left join public.uza f
                on st_within(a.geom, f.geom));
    """
    print("joining conflated point and line layers")
    db.execute(query)


def create_point_cols():
    deficiencies = [
        "bridge",
        "vul_user",
        "ksi",
        "crrate",
        "sidewalk",
        "missing_bike_fac",
        "transit_rt",
        "tti",
        "pti",
        "bottleneck",
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


def copy_megajoin(scenarios: list):
    for scenario in scenarios:
        query = f"""drop table if exists point_assignment.scenario_{scenario};
        create table point_assignment.scenario_{scenario} as(
        select * from point_assignment.megajoin
        )"""
        db.execute(query)
        print(f"setting up scenario {scenario}")


def assign_points(table: str, point_col: str, point: int, where_statement: str):
    query = f"""
    UPDATE point_assignment.{table} SET {point_col} = {point} WHERE {where_statement}
    """
    db.execute(query)


def critical_flag(table: str):
    query = f"""
        alter table point_assignment.{table} add column if not exists critical int;
        UPDATE point_assignment.{table } SET critical = 1 WHERE bridge_rating <= 20;"""
    db.execute(query)

def total_points(table:str):
    query = f"""
    alter table point_assignment.{table} add column if not exists total int;
    UPDATE point_assignment.{table} set total = bridge_pts + vul_user_pts + ksi_pts + crrate_pts + sidewalk_pts + missing_bike_fac_pts + tti_pts + pti_pts + bottleneck_pts + transit_rt_pts;
    """
    db.execute(query)

def assign_scenario_a(table: str):
    """scenario a is the "baseline" scenario upon which others are built. """

    assign_points(table, "bridge_pts", 1, "bridge_rating between 20 and 50")
    assign_points(table, "bridge_pts", 2, "bridge_rating <= 20")
    assign_points(table, "vul_user_pts", 2, "vul_crash > 0;")
    assign_points(table, "ksi_pts", 2, "ksi > 0;")
    assign_points(table, "crrate_pts", 1, "crrate between 1256 and 2025;")
    assign_points(table, "crrate_pts", 2, "crrate > 2025;")
    assign_points(
        table,
        "sidewalk_pts",
        1,
        "sw_ratio between .01 and .5 and lsad_type = 'Urbanized Area';",
    )
    assign_points(
        table, "sidewalk_pts", 2, "sw_ratio < .01 and lsad_type = 'Urbanized Area';"
    )
    assign_points(
        table, "sidewalk_pts", 1, "sw_ratio < .01 and lsad_type != 'Urbanized Area';"
    )
    assign_points(table, "missing_bike_fac_pts", 1, "bikefacili = 'Sharrows'")
    assign_points(table, "missing_bike_fac_pts", 2, "bikefacili = 'No Accomodation';")
    assign_points(
        table,
        "tti_pts",
        1,
        "lsad_type = 'Urbanized Area' and ttiwkd0708 >= 1.5 or ttiwkd0809 >=1.5 or ttiwkd1617 >=1.5 or ttiwkd1718 >= 1.5;",
    )
    assign_points(
        table,
        "tti_pts",
        1,
        "lsad_type != 'Urbanized Area' and ttiwkd0708 between 1.2 and 1.5 or ttiwkd0809 between 1.2 and 1.5 or ttiwkd1617 between 1.2 and 1.5 or ttiwkd1718 between 1.2 and 1.5;",
    )
    assign_points(
        table,
        "tti_pts",
        2,
        "lsad_type != 'Urbanized Area' and ttiwkd0708 >= 1.5 or ttiwkd0809 >=1.5 or ttiwkd1617 >=1.5 or ttiwkd1718 >= 1.5;",
    )
    assign_points(
        table,
        "pti_pts",
        1,
        "lsad_type = 'Urbanized Area' and ptiwkd0708 >= 3 or ptiwkd0809 >=3 or ptiwkd1617 >=3 or ptiwkd1718 >=3;",
    )
    assign_points(
        table,
        "pti_pts",
        1,
        "lsad_type != 'Urbanized Area' and ptiwkd0708 between 2 and 3 or ptiwkd0809 between 2 and 3 or ptiwkd1617 between 2 and 3 or ptiwkd1718 between 2 and 3;",
    )
    assign_points(
        table,
        "pti_pts",
        2,
        "lsad_type != 'Urbanized Area' and ptiwkd0708 >= 3 or ptiwkd0809 >=3 or ptiwkd1617 >=3 or ptiwkd1718 >=3;",
    )
    assign_points(table, "bottleneck_pts", 1, "inrixxd=0;")
    assign_points(table, "transit_rt_pts", 1, "line is not null;")
    assign_points(table, "transit_rt_pts", 2, "bridge_rating between 20 and 50")
    critical_flag(table)
    total_points(table)

def assign_scenario_b1(table:str):
    # scenario a is "baseline" for all others, so generate the same points for it, then only update what's necessary 
    assign_scenario_a(table) 
    assign_points(table, "tti_pts", 1, "lsad_type = 'Urbanized Area' and ttiwkd0708 between 1.2 and 1.5 or ttiwkd0809 between 1.2 and 1.5 or ttiwkd1617 between 1.2 and 1.5 or ttiwkd1718 between 1.2 and 1.5;")
    assign_points(table, "tti_pts", 2, "lsad_type = 'Urbanized Area' and ttiwkd0708 >1.5 or ttiwkd0809 >1.5 or ttiwkd1617 >1.5 or ttiwkd1718 >1.5;") 
    assign_points(table, "pti_pts", 1, "lsad_type = 'Urbanized Area' and ptiwkd0708 between 2 and 3 or ptiwkd0809 between 2 and 3 or ptiwkd1617 between 2 and 3 or ptiwkd1718 between 2 and 3;")
    assign_points(table, "pti_pts", 2, "lsad_type = 'Urbanized Area' and ptiwkd0708 >3 or ptiwkd0809 >3 or ptiwkd1617 >3 or ptiwkd1718 >3;")

if __name__ == "__main__":
    megajoin()
    create_point_cols()
    copy_megajoin(scenarios)
    assign_scenario_a("scenario_a")
    assign_scenario_b1("scenario_b1")
