from pg_data_etl import Database
import glob
import pandas as pd
import geopandas as gpd
import os
from dotenv import load_dotenv
from pathlib import Path
from planbelt.plan_belt.census import census_pull
import pandas as pd

load_dotenv()

api_key = os.getenv("api_key")  # census api key
db = Database.from_config("mercer", "omad")
gis_db = Database.from_config("gis", "gis")
data_folder = Path(os.getenv("data_root"))  # path to g drive folder'

# this should be whatever you'd like to clip to. extended mercer's boundaries by 10 meters to account for two county line roads
mask_layer = gis_db.gdf(
    "select st_buffer(shape, 10) as shape from boundaries.countyboundaries where co_name = 'Mercer' and state_name = 'New Jersey'",
    geom_col="shape",
)
mask_layer = mask_layer.to_crs(26918)


def import_and_clip(
    sql_query: str,
    geom_col: str,
    sql_tablename_output: str,
    explode=True,
    gpd_kwargs={"if_exists": "replace"},
    db_to_use=gis_db,
):
    gdf = db_to_use.gdf(sql_query, geom_col)
    gdf = gdf.to_crs(26918)
    clipped = gpd.clip(gdf, mask_layer, keep_geom_type=True)
    db.import_geodataframe(
        clipped, sql_tablename_output, gpd_kwargs=gpd_kwargs, explode=explode
    )
    print(f"importing {sql_tablename_output}, please wait...")


def import_shapefile(folder_string: str, output_string: str = "", clip: bool = True):
    """
    imports a shapefile from the data folder.

    :param folder_string: name of subfolder with  shapefile(s)
    :param output_string: what you'd like it to be called in postgres
    :param clip: bool, clip to muni boundry or not"""

    folder = data_folder / folder_string
    for shapefile in glob.iglob(
        f"{folder}/*.[sS][hH][pP]"
    ):  # brackets deal with case differences between different folders (shp vs SHP)
        file = Path(shapefile)
        print(f"processing {file.stem}, please wait...")
        gdf = gpd.read_file(shapefile)
        gdf = gdf.to_crs(26918)
        if clip == True:
            clipped = gpd.clip(gdf, mask_layer)
            db.import_geodataframe(
                clipped,
                output_string + str(file.stem).lower(),
                explode=True,
                gpd_kwargs={"if_exists": "replace"},
            )
        else:
            db.import_geodataframe(
                gdf,
                output_string + str(file.stem).lower(),
                explode=True,
                gpd_kwargs={"if_exists": "replace"},
            )
        print(f"{output_string} imported successfully")


def create_high_priority_geometry():
    """
    Creates a large polygon that encompasses disadvantaged or important areas to prioritize for some of the scenarios.

    """
    dropq = """drop table if exists disadvantaged CASCADE;"""
    db.execute(dropq)

    census_tables = {
        "B01001_001E": "total",
        "B08014_002E": "zero_car",
        "S1810_C02_001E": "disabled",
        "B09001_001E": "youth",
        "S0101_C01_030E": "older_adults",
        "S1701_C01_042E": "low_income",
        "B02001_002E": "racial_minority",
        "B03002_012E": "ethnic_minority",
    }
    df = pd.DataFrame()
    count = 0
    for key in census_tables:
        table_type = ""
        if key[0] == "B":
            table_type = ""
        if key[0] == "S":
            table_type = "/subject"
        temp_table = census_pull.CensusTable(
            "2021", "acs", "acs5", key, "34", "021", api_key, "*", table_type
        )
        column_header = census_tables[key]
        if count == 0:
            df = temp_table.df
            column_to_move = df.pop(f"{key}")
            df.insert(4, f"{key}", column_to_move)

        else:
            specific_column = temp_table.df[[f"{key}"]].copy()
            df = pd.concat([df, specific_column], axis=1)

        df = df.rename(columns={f"{key}": f"{column_header}"})
        count += 1
    db.import_dataframe(df, "disadvantaged", {"if_exists": "replace"})

    query = """
    create or replace view job_density as (
        with netsjobs as (
            select a.geoid, a.geom, sum(b.emp15) as jobs from census_tracts_2020 a 
            inner join nets_2015 b 
            on st_within(b.geom, a.geom)
            group by a.geom, a.geoid 
            order by jobs desc)
        select *, jobs/(st_area(geom)*0.00000038610215855) as jobs_sq_mile 
        from netsjobs);
    create or replace view density as 
    select 
        total::int/(st_area(b.geom)*0.00000038610215855) as pop_density,
        zero_car::int/(st_area(b.geom)*0.00000038610215855) as zero_car_density,
        disabled::int/(st_area(b.geom)*0.00000038610215855) as disability_density,
        youth::int/(st_area(b.geom)*0.00000038610215855) as youth_density,
        older_adults::int/(st_area(b.geom)*0.00000038610215855)as older_adults_density,
        low_income::int/(st_area(b.geom)*0.00000038610215855) as low_income_density,
        ethnic_minority::int/(st_area(b.geom)*0.00000038610215855) as ethnic_minority_density,
        racial_minority::int/(st_area(b.geom)*0.00000038610215855) as racial_minority_density,
        c.jobs_sq_mile as job_density,
        b.geom
    from disadvantaged a
        inner join census_tracts_2020 b 
        on concat(a.state, a.county, a.tract) = b.geoid 
        left join job_density c 
        on st_within(b.geom, c.geom);
    create or replace view above_sds as 
    select * from density
    where pop_density > (select stddev(pop_density) + avg(pop_density) from density)
    or zero_car_density > (select stddev(zero_car_density) + avg(zero_car_density) from density)
    or disability_density > (select stddev(disability_density) + avg(disability_density) from density)
    or youth_density > (select stddev(youth_density) + avg(youth_density) from density)
    or older_adults_density > (select stddev(older_adults_density) + avg(older_adults_density) from density)
    or pop_density > (select stddev(pop_density) + avg(pop_density) from density)
    or low_income_density > (select stddev(low_income_density) + avg(low_income_density) from density);
    create or replace view density_simp as 
        select st_union(st_buffer(a.geom, 402)) as geom from above_sds a;
    create or replace view bus_buffers as 
        select st_union(st_buffer(a.geom, 402)) as geom from transit_stops_clipped a;
    create or replace view rail_buffers as 
        select st_union(st_buffer(b.geom, 1608)) as geom  from passengerrailstations b;
    create or replace view essentialbuffers as 
        select st_union(st_buffer(a.geom, 402)) as geom  from essentialservices a;
    create or replace view recreationbuffer as
        select st_union(st_buffer(a.geom, 402)) as geom  from dvrpclu2015 a
        where a.lu15catn = 'Recreation';
    drop table if exists high_priority;
    create table high_priority as	
    SELECT ST_Union( 
    ARRAY[ 
        (SELECT geom FROM density_simp), 
        (SELECT geom FROM bus_buffers), 
       (SELECT geom FROM rail_buffers), 
       (SELECT geom FROM essentialbuffers), 
        (SELECT geom FROM recreationbuffer )]) as geom; 
    """
    db.execute(query)


if __name__ == "__main__":
    import_and_clip("select * from transportation.njdot_lrs", "shape", "lrs_clipped")
    import_and_clip(
        "select * from transportation.nj_centerline",
        "shape",
        "nj_centerline",
    )
    import_and_clip(
        "select * from transportation.pedestriannetwork_gaps",
        "shape",
        "sidewalk_gaps_clipped",
    )
    import_and_clip(
        "select * from transportation.njtransit_transitstops",
        "shape",
        "transit_stops_clipped",
    )
    import_and_clip(
        "select * from transportation.cmp2019_inrix_traveltimedata",
        "shape",
        "inrix_2019_clipped",
    )
    import_and_clip(
        "select * from transportation.cmp2019_nj_crashfrequencyseverity",
        "shape",
        "cmp_crashfreqseverity_2019_clipped",
    )
    import_and_clip(
        "select * from transportation.cmp2019_focus_intersection_bottlenecks",
        "shape",
        "cmp_focus_bottleneck_2019_clipped",
    )
    import_and_clip("select * from demographics.ipd_2020", "shape", "ipd_2020_clipped")
    import_and_clip(
        "select * from transportation.pedestriannetwork_points where status = 'MISSING'",
        "shape",
        "missing_curb_ramps",
    )
    import_and_clip(
        "select * from transportation.pedestriannetwork_lines", "shape", "ped_network"
    )
    import_and_clip(
        "select * from transportation.circuittrails", "shape", "circuit_trails"
    )
    import_and_clip(
        "select objectid, verif_by, verif_on, multi_use, surface, comments_dvrpc, county, name, verif_status, owner, ST_Force2D(shape) as shape, miles from transportation.all_trails",
        "shape",
        "all_trails",
    )
    import_and_clip(
        "select lu15cat, lu15catn, acres, lu15dev, mixeduse, st_force2d(shape) as shape from planning.dvrpc_landuse_2015",
        "shape",
        "dvrpclu2015",
        explode=False,
    )
    import_and_clip(
        "select * from demographics.forecast_2015to2050_taz",
        "shape",
        "dem_emp_forecast_2015",
        explode=False,
    )
    import_and_clip(
        "select * from transportation.njtransit_transitroutes",
        "shape",
        "nj_transit_routes",
    )
    import_and_clip(
        "select * from transportation.lts_network where bikefacili = 'No Accomodation' or bikefacili ='Sharrows' ",
        "shape",
        "lts_deficient_facils",
    )
    import_and_clip(
        "select * from boundaries.urbanareas_nj",
        "shape",
        "uza",
        explode=True,
    )
    import_and_clip(
        "select * from demographics.census_tracts_2020", "shape", "census_tracts_2020"
    )
    import_and_clip(
        "select * from economy.nets_2015 where coname = 'Mercer'", "shape", "nets_2015"
    )
    import_and_clip(
        "select * from transportation.passengerrailstations",
        "shape",
        "passengerrailstations",
    )
    import_and_clip(
        "select * from planning.eta_essentialservicespts", "shape", "essentialservices"
    )
    # generic shapefile imports
    import_shapefile("ModelVolumes", "model_vols")
    import_shapefile("JobAccess", clip=False)
    import_shapefile("NJDOT2021_ADT")
    import_shapefile("TravelTimes")
    import_shapefile("MercerCountyRoads")
    import_shapefile("MercerBikeFacilities")
    import_shapefile("CrashSegment")
    import_shapefile("Bottlenecks")  # this is a shapefile that tom made
    import_shapefile("TransitFreq")
    create_high_priority_geometry()
