from pg_data_etl import Database
from dotenv import load_dotenv
import os

load_dotenv()
api_key = os.getenv("api_key")

db = Database.from_config("mercer", "omad")

scenarios = ["a", "b1", "b2", "c", "d", "e", "matts"]


def megajoin():
    query = """
    drop schema if exists point_assignment CASCADE;
    create schema point_assignment;
    create table point_assignment.megajoin as (
        select 
            a.*, 
            b.inrixxd, 
            c.lsad_type
        from rejoined.all a
            left join public.bottlenecks b
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


def total_points(table: str):
    query = f"""
    alter table point_assignment.{table} add column if not exists total int;
    UPDATE point_assignment.{table} set total = vulusercrrate_pts + ksicrrate_pts + crrate_pts + sidewalk_pts + missing_bike_fac_pts + tti_pts + pti_pts + bottleneck_pts + transit_rt_pts;
    """
    db.execute(query)


def avg_and_sd(column: str, table: str):
    """returns average and sd, calculated in pg"""
    avg = db.query_as_singleton(f"select avg({column}) from point_assignment.{table}")
    sd = db.query_as_singleton(f"select stddev({column}) from point_assignment.{table}")
    return [avg, sd]


def assign_scenario_a(table: str):
    """scenario a is the "baseline" scenario upon which others are built.

    calculates the average and standard deviations of each column then updates score where crashes between .5 and 1.5 sd, and also where they're > 1.5 sd.
    also assigns points in accordance with scenario a deficiency threshold table
    """

    vcr = avg_and_sd("vulcrrate", table)
    ksicr = avg_and_sd("ksicrrate", table)
    cr = avg_and_sd("crrate", table)
    assign_points(
        table,
        "vulusercrrate_pts",
        1,
        f"vulcrrate between {vcr[0] + .5*vcr[1]} and {vcr[0] + 1.5 * vcr[1]};",
    )
    assign_points(
        table, "vulusercrrate_pts", 2, f"vulcrrate > {vcr[0] + 1.5 * vcr[1]};"
    )
    assign_points(
        table,
        "ksicrrate_pts",
        1,
        f"ksicrrate between {ksicr[0] + .5*ksicr[1]} and {ksicr[0] + 1.5 * ksicr[1]};",
    )
    assign_points(
        table, "ksicrrate_pts", 2, f"ksicrrate > {ksicr[0] + 1.5 * ksicr[1]};"
    )
    assign_points(
        table,
        "crrate_pts",
        1,
        f"crrate between {cr[0] + .5*cr[1]} and {cr[0] + 1.5 * cr[1]};",
    )
    assign_points(table, "crrate_pts", 2, f"crrate > ({cr[0] + 1.5 * cr[1]});")
    assign_points(
        table,
        "sidewalk_pts",
        1,
        "sw_ratio between .01 and .5 and lsad_type = 'Urbanized Area';",
    )
    assign_points(
        table, "sidewalk_pts", 2, "sw_ratio < .01 and lsad_type = 'Urbanized Area';"
    )
    assign_points(table, "sidewalk_pts", 1, "sw_ratio < .01 and lsad_type is null;")
    assign_points(table, "missing_bike_fac_pts", 1, "bikefacili = 'Sharrows'")
    assign_points(table, "missing_bike_fac_pts", 2, "bikefacili = 'No Accomodation';")
    assign_points(
        table,
        "tti_pts",
        1,
        "(lsad_type = 'Urbanized Area') and (ttiwkd0708 >= 1.5 or ttiwkd0809 >=1.5 or ttiwkd1617 >=1.5 or ttiwkd1718 >= 1.5);",
    )
    assign_points(
        table,
        "tti_pts",
        1,
        "(lsad_type is null) and (ttiwkd0708 between 1.2 and 1.5 or ttiwkd0809 between 1.2 and 1.5 or ttiwkd1617 between 1.2 and 1.5 or ttiwkd1718 between 1.2 and 1.5);",
    )
    assign_points(
        table,
        "tti_pts",
        2,
        "(lsad_type is null) and (ttiwkd0708 >= 1.5 or ttiwkd0809 >=1.5 or ttiwkd1617 >=1.5 or ttiwkd1718 >= 1.5);",
    )
    assign_points(
        table,
        "pti_pts",
        1,
        "(lsad_type = 'Urbanized Area') and (ptiwkd0708 >= 3 or ptiwkd0809 >=3 or ptiwkd1617 >=3 or ptiwkd1718 >=3);",
    )
    assign_points(
        table,
        "pti_pts",
        1,
        "(lsad_type is null) and (ptiwkd0708 between 2 and 3 or ptiwkd0809 between 2 and 3 or ptiwkd1617 between 2 and 3 or ptiwkd1718 between 2 and 3);",
    )
    assign_points(
        table,
        "pti_pts",
        2,
        "(lsad_type is null) and (ptiwkd0708 >= 3 or ptiwkd0809 >=3 or ptiwkd1617 >=3 or ptiwkd1718 >=3);",
    )
    assign_points(table, "bottleneck_pts", 1, "inrixxd=0;")
    assign_points(table, "transit_rt_pts", 1, "line is not null;")
    assign_points(table, "transit_rt_pts", 2, "busfreq >=3 or busfreq2 >=3")
    total_points(table)


def assign_scenario_matts(table: str):
    assign_scenario_a(table)
    assign_points(table, "bottleneck_pts", 2, "inrixxd!=0;")
    total_points(table)


def assign_scenario_b1(table: str):
    # scenario a is "baseline" for all others, so generate the same points for it, then only update what's necessary
    assign_scenario_a(table)
    assign_points(
        table,
        "tti_pts",
        1,
        "(lsad_type = 'Urbanized Area') and (ttiwkd0708 between 1.2 and 1.5 or ttiwkd0809 between 1.2 and 1.5 or ttiwkd1617 between 1.2 and 1.5 or ttiwkd1718 between 1.2 and 1.5);",
    )
    assign_points(
        table,
        "tti_pts",
        2,
        "(lsad_type = 'Urbanized Area') and (ttiwkd0708 >1.5 or ttiwkd0809 >1.5 or ttiwkd1617 >1.5 or ttiwkd1718 >1.5);",
    )
    assign_points(
        table,
        "pti_pts",
        1,
        "(lsad_type = 'Urbanized Area') and (ptiwkd0708 between 2 and 3 or ptiwkd0809 between 2 and 3 or ptiwkd1617 between 2 and 3 or ptiwkd1718 between 2 and 3);",
    )
    assign_points(
        table,
        "pti_pts",
        2,
        "(lsad_type = 'Urbanized Area') and (ptiwkd0708 >3 or ptiwkd0809 >3 or ptiwkd1617 >3 or ptiwkd1718 >3);",
    )
    total_points(table)


def assign_scenario_c(table: str):
    pass


if __name__ == "__main__":
    megajoin()
    create_point_cols()
    copy_megajoin(scenarios)
    assign_scenario_a("scenario_a")
    assign_scenario_b1("scenario_b1")
    assign_scenario_matts("scenario_matts")
    print(assign_scenario_c("scenario_c"))
