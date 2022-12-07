from cmath import exp
from pg_data_etl import Database
import glob
import pandas as pd
import geopandas as gpd
import os
from dotenv import load_dotenv
from pathlib import Path
from shapely.geometry import Point

load_dotenv()

db = Database.from_config("mercer", "omad")
gis_db = Database.from_config("gis", "gis")
conflate_db = Database.from_config("conflate", "conflate")
data_folder = Path(os.getenv("data_root"))  # path to g drive folder'

# this should be whatever you'd like to clip to. extended mercer's boundaries by 10 meters to account for two county line roads
mask_layer = gis_db.gdf(
    "select st_buffer(shape, 10) as shape from boundaries.countyboundaries where co_name = 'Mercer' and state_name = 'New Jersey'",
    geom_col="shape",
)
mask_layer = mask_layer.to_crs(26918)


def import_and_clip(
    sql_query=str,
    geom_col=str,
    sql_tablename_output=str,
    gpd_kwargs={"if_exists": "replace"},
    explode=True,
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


def import_safety_voyager():
    sv = data_folder / "Safety Voyager"
    gdf_list = []
    for folder in glob.iglob(f"{sv}/*"):
        path = Path(folder)
        for csv in glob.iglob(f"{path}/*.csv"):
            print(f"processing {path.stem} safety voyager data...")
            df = pd.read_csv(csv)
            geometry = [Point(xy) for xy in zip(df.Longitude, df.Latitude)]
            df = df.drop(["Longitude", "Latitude"], axis=1)
            temp_gdf = gpd.GeoDataFrame(df, geometry=geometry, crs=4326)
            gdf_list.append(temp_gdf)
    print("combining safety voyager datasets...")
    gdf = gpd.GeoDataFrame(pd.concat(gdf_list, ignore_index=True))
    gdf = gdf.to_crs(26918)
    db.import_geodataframe(
        gdf, "safety_voyager", explode=True, gpd_kwargs={"if_exists": "replace"}
    )
    print("safety voyager imported successfully")


def import_bridges():
    # imports spatial bridge data from NJDOT shapefile
    import_shapefile("Bridges", "")

    suff_rating = data_folder / "Bridges" / "sufficient ratings.xlsx"
    df = pd.read_excel(suff_rating, sheet_name="Sheet1")
    df["Asset Name"] = df["Asset Name"].str.split("\s+\(").str[0]
    db.import_dataframe(df, "bridges_excel", df_import_kwargs={"if_exists": "replace"})
    print("bridge excel file imported successfully")
    query = """select nb.*, be.parent_asset, be.asset_name, be.unofficial_sufficiency_rating from njdot_bridges_2019 nb 
            inner join bridges_excel be 
            on be.asset_name = nb.structure_ 
            where "owner" = 'MERCER COUNTY' """
    db.gis_make_geotable_from_query(query, "bridges_joined", "Point", 26918)
    dropquery = """drop table if exists bridges_excel, njdot_bridges_2019"""
    db.execute(dropquery)
    print("join to bridge excel sheet successful")


if __name__ == "__main__":
    import_and_clip("select * from transportation.njdot_lrs", "shape", "lrs_clipped")
    import_and_clip(
        "select * from public.nj_centerline",
        "geom",
        "nj_centerline",
        db_to_use=conflate_db,
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

    # # generic shapefile imports
    import_shapefile("ModelVolumes", "model_vols")
    import_shapefile("JobAccess", clip=False)
    import_shapefile("NJDOT2021_ADT")
    import_shapefile("Pavement Condition/Pavement Condition Index")
    import_shapefile("TravelTimes")
    import_shapefile("MercerCountyRoads")
    import_shapefile("MercerBikeFacilities")
    import_shapefile("CrashSegment")
    import_shapefile("Bottlenecks")  # this is a shapefile that tom made

    # shapefiles that require more specific handling (e.g., joining to a CSV)
    import_safety_voyager()
    import_bridges()
