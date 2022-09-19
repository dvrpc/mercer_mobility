# you might need to pip install pg_data_etl , i'm not sure that it made it into the .env file. 
# note to use my forked file, not Aaron's original (unless he updates with my forked change which corrects shape issue)
from pg_data_etl import Database
import glob
import pandas as pd
import geopandas as gpd
import os
from dotenv import load_dotenv
from pathlib import Path
from shapely.geometry import Point
load_dotenv()

db = Database.from_config("mercer", "localhost")
gis_db = Database.from_config("gis", "gis")
data_folder = Path(os.getenv("data_root")) #path to g drive folder'

# this should be whatever you'd like to clip to
mask_layer = gis_db.gdf("select * from boundaries.countyboundaries where co_name = 'Mercer' and state_name = 'New Jersey'", geom_col= "shape")
mask_layer = mask_layer.to_crs(26918)


def import_and_clip(sql_query = str, geom_col =str, sql_tablename_output = str):
    gdf = gis_db.gdf(sql_query, geom_col)
    gdf = gdf.to_crs(26918)
    clipped = gpd.clip(gdf, mask_layer)
    db.import_geodataframe(clipped, sql_tablename_output, explode=True, gpd_kwargs={'if_exists':'replace'})
    print(f"importing {sql_tablename_output}, please wait...")


#import model volume data (g drive)
def import_model_volumes():
    model_folder = data_folder / 'ModelVolumes'
    for shapefile in glob.iglob(f'{model_folder}/*.SHP'):
        file = Path(shapefile)
        print(f"processing {file.stem}, please wait...")
        gdf = gpd.read_file(shapefile)
        gdf = gdf.to_crs(26918)
        clipped = gpd.clip(gdf, mask_layer)
        db.import_geodataframe(clipped, "model_vol_" + str(file.stem).lower(), explode=True, gpd_kwargs={'if_exists':'replace'})
    print("model volumes imported successfully")

#import job access data (sarah's email)

#bridges (G)

#adt data (G)
def import_adt():
    adt = data_folder / 'NJDOT2021_ADT'
    for shapefile in glob.iglob(f'{adt}/*.shp'):
        file = Path(shapefile)
        print(f"importing {file.stem}, please wait...")
        gdf = gpd.read_file(shapefile)
        gdf = gdf.to_crs(26918)
        clipped = gpd.clip(gdf, mask_layer)
        db.import_geodataframe(clipped, str(file.stem).lower(), explode=True, gpd_kwargs={'if_exists':'replace'})
    print("adt imported successfully")

#pavement condition (G)
def import_pavement_conditions():
    pci = data_folder / 'Pavement Condition' / 'Pavement Condition Index' 
    print(pci)
    for shapefile in glob.iglob(f'{pci}/*.shp'):
        file = Path(shapefile)
        print(f"processing {file.stem}, please wait...")
        gdf = gpd.read_file(shapefile)
        gdf = gdf.to_crs(26918)
        clipped = gpd.clip(gdf, mask_layer)
        db.import_geodataframe(clipped, str(file.stem).lower(), explode=True, gpd_kwargs={'if_exists':'replace'})
    print("pavement condition shapefile imported successfully")

#safety voyager (G)
def import_safety_voyager():
    sv = data_folder / 'Safety Voyager'
    for folder in glob.iglob(f'{sv}/*'): 
        path = Path(folder)
        for csv in glob.iglob (f'{path}/*.csv'):
            print(f"processing {path.stem} safety voyager data...")
            df = pd.read_csv(csv)
            geometry = [Point(xy) for xy in zip(df.Longitude, df.Latitude)]
            df = df.drop(['Longitude','Latitude'], axis=1)
            gdf = gpd.GeoDataFrame(df, geometry=geometry, crs = 26918)
            db.import_geodataframe(gdf, "sv_" + str(path.stem).lower(), explode=True, gpd_kwargs={'if_exists':'replace'})
    print("safety voyager imported successfully")

if __name__ == "__main__":
    # import_and_clip("select * from transportation.njdot_lrs", "shape", "lrs_clipped")
    # import_and_clip("select * from transportation.pedestriannetwork_gaps", "shape", "sidewalk_gaps_clipped")
    # import_and_clip("select * from transportation.njtransit_transitstops", "shape", "transit_stops_clipped")
    # import_and_clip("select * from transportation.cmp2019_inrix_traveltimedata", "shape", "inrix_2019_clipped")
    # import_and_clip("select * from transportation.cmp2019_nj_crashfrequencyseverity", "shape", "cmp_crashfreqseverity_2019_clipped")
    # import_and_clip("select * from transportation.cmp2019_focus_intersection_bottlenecks", "shape", "cmp_focus_bottleneck_2019_clipped")
    # import_model_volumes()
    # import_adt()
    # import_safety_voyager()
    import_pavement_conditions()