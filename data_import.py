# you might need to pip install pg_data_etl , i'm not sure that it made it into the .env file. 
# note to use my forked file, not Aaron's original (unless he updates with my forked change which corrects shape issue)
from xml.parsers.expat import model
from pg_data_etl import Database
import glob
import geopandas as gpd
import os
from dotenv import load_dotenv
from pathlib import Path
load_dotenv()

db = Database.from_config("mercer", "localhost")
gis_db = Database.from_config("gis", "gis")
data_folder = Path(os.getenv("data_root")) #path to g drive folder

# this should be whatever you'd like to clip to
mask_layer = gis_db.gdf("select * from boundaries.countyboundaries where co_name = 'Mercer' and state_name = 'New Jersey'", geom_col= "shape")
mask_layer = mask_layer.to_crs(26918)


def import_and_clip(sql_query = str, geom_col =str, sql_tablename_output = str):
    gdf = gis_db.gdf(sql_query, geom_col)
    gdf = gdf.to_crs(26918)
    clipped = gpd.clip(gdf, mask_layer)
    db.import_geodataframe(clipped, sql_tablename_output, explode=True)


#import model volume data (g drive)
def import_model_volumes():
    model_folder = data_folder / 'ModelVolumes'
    for file in model_folder.glob('*.DBF'):
        print(file)
        # gdf = gpd.read_file(filepath)
        # gdf = gdf.to_crs(26918)
        # clipped = gpd.clip(gdf, mask_layer)
        # db.import_geodataframe(clipped, sql_tablename_output, explode=True)
    return model_folder

#import job access data (sarah's email)

#bridges (G)

#adt data (G)

#pavement condition (G)

#safety voyager (G)

if __name__ == "__main__":
    # import_and_clip("select * from transportation.njdot_lrs", "shape", "lrs_clipped")
    # import_and_clip("select * from transportation.pedestriannetwork_gaps", "shape", "sidewalk_gaps_clipped")
    # import_and_clip("select * from transportation.njtransit_transitstops", "shape", "transit_stops_clipped")
    # import_and_clip("select * from transportation.cmp2019_inrix_traveltimedata", "shape", "inrix_2019_clipped")
    # import_and_clip("select * from transportation.cmp2019_nj_crashfrequencyseverity", "shape", "cmp_crashfreqseverity_2019_clipped")
    # import_and_clip("select * from transportation.cmp2019_focus_intersection_bottlenecks", "shape", "cmp_focus_bottleneck_2019_clipped")
    print(import_model_volumes())
    print("imported!")