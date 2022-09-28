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


def import_and_clip(sql_query = str, geom_col =str, sql_tablename_output = str, gpd_kwargs = {'if_exists':'replace'}):
    gdf = gis_db.gdf(sql_query, geom_col)
    gdf = gdf.to_crs(26918)
    clipped = gpd.clip(gdf, mask_layer)
    db.import_geodataframe(clipped, sql_tablename_output, explode=True, gpd_kwargs = gpd_kwargs )
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
def import_jobs():
    jobs = data_folder / 'JobAccess'
    for shapefile in glob.iglob(f'{jobs}/*.shp'):
        file = Path(shapefile)
        print(f"importing {file.stem}, please wait...")
        gdf = gpd.read_file(shapefile, mask=mask_layer)
        gdf = gdf.to_crs(26918)
        db.import_geodataframe(gdf, str(file.stem).lower(), explode=True, gpd_kwargs={'if_exists':'replace'})
    print("job access imported successfully")

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
    gdf_list = []
    for folder in glob.iglob(f'{sv}/*'): 
        path = Path(folder)
        for csv in glob.iglob (f'{path}/*.csv'):
            print(f"processing {path.stem} safety voyager data...")
            df = pd.read_csv(csv)
            geometry = [Point(xy) for xy in zip(df.Longitude, df.Latitude)]
            df = df.drop(['Longitude','Latitude'], axis=1)
            temp_gdf = gpd.GeoDataFrame(df, geometry=geometry, crs = 4326)
            gdf_list.append(temp_gdf)
    print("combining safety voyager datasets...")
    gdf = gpd.GeoDataFrame( pd.concat( gdf_list, ignore_index=True) )
    gdf.to_crs(26918)
    db.import_geodataframe(gdf, "safety_voyager", explode=True, gpd_kwargs={'if_exists':'replace'})
    print("safety voyager imported successfully")

    #traveltimes (G)
def import_travel_times():
    tt = data_folder / 'TravelTimes'
    print(tt)
    for shapefile in glob.iglob(f'{tt}/*.shp'):
        file = Path(shapefile)
        print(f"processing {file.stem}, please wait...")
        gdf = gpd.read_file(shapefile)
        gdf = gdf.to_crs(26918)
        clipped = gpd.clip(gdf, mask_layer)
        db.import_geodataframe(clipped, str(file.stem).lower(), explode=True, gpd_kwargs={'if_exists':'replace'})
    print("travel time shapefile imported successfully")

def import_mercer_roads():
    roads = data_folder / 'MercerCountyRoads'
    print(roads)
    for shapefile in glob.iglob(f'{roads}/*.shp'):
        file = Path(shapefile)
        print(f"processing {file.stem}, please wait...")
        gdf = gpd.read_file(shapefile)
        gdf = gdf.to_crs(26918)
        clipped = gpd.clip(gdf, mask_layer)
        db.import_geodataframe(clipped, str(file.stem).lower(), explode=True, gpd_kwargs={'if_exists':'replace'})
    print("mercer jurisdiction roads shapefile imported successfully")

def import_bridges():
    #imports spatial bridge data from NJDOT shapefile
    bridges = data_folder / 'Bridges'
    print(bridges)
    for shapefile in glob.iglob(f'{bridges}/*.shp'):
        file = Path(shapefile)
        print(f"processing {file.stem}, please wait...")
        gdf = gpd.read_file(shapefile)
        gdf = gdf.to_crs(26918)
        clipped = gpd.clip(gdf, mask_layer)
        db.import_geodataframe(clipped, str(file.stem).lower(), explode=True, gpd_kwargs={'if_exists':'replace'})
    print("bridges shapefile imported successfully")
    suff_rating = data_folder / 'Bridges' / 'sufficient ratings.xlsx'
    df = pd.read_excel(suff_rating,sheet_name='Sheet1')
    df['Asset Name']= df['Asset Name'].str.split("\s+\(").str[0]
    db.import_dataframe(df, "bridges_excel", df_import_kwargs={'if_exists':'replace'})
    print("bridge excel file imported successfully")
    query = """select nb.*, be.parent_asset, be.asset_name, be.unofficial_sufficiency_rating from njdot_bridges_2019 nb 
            inner join bridges_excel be 
            on be.asset_name = nb.structure_ 
            where "owner" = 'MERCER COUNTY' """
    db.gis_make_geotable_from_query(query, "bridges_joined", "Point", 26918)
    dropquery = """drop table if exists bridges_excel, njdot_bridges_2019"""
    db.execute(dropquery)
    
if __name__ == "__main__":
    # import_and_clip("select * from transportation.njdot_lrs", "shape", "lrs_clipped")
    # import_and_clip("select * from transportation.pedestriannetwork_gaps", "shape", "sidewalk_gaps_clipped")
    # import_and_clip("select * from transportation.njtransit_transitstops", "shape", "transit_stops_clipped")
    # import_and_clip("select * from transportation.cmp2019_inrix_traveltimedata", "shape", "inrix_2019_clipped")
    # import_and_clip("select * from transportation.cmp2019_nj_crashfrequencyseverity", "shape", "cmp_crashfreqseverity_2019_clipped")
    # import_and_clip("select * from transportation.cmp2019_focus_intersection_bottlenecks", "shape", "cmp_focus_bottleneck_2019_clipped")
    # import_and_clip("select * from demographics.ipd_2020", "shape", "ipd_2020_clipped")
    # import_and_clip("select * from transportation.pedestriannetwork_points where status = 'MISSING'", "shape", "missing_curb_ramps")
    # import_and_clip("select * from transportation.pedestriannetwork_lines", "shape", "ped_network")
    # import_and_clip("select * from transportation.circuittrails", "shape", "circuit_trails")
    # import_and_clip("select objectid, verif_by, verif_on, multi_use, surface, comments_dvrpc, county, name, verif_status, owner, ST_Force2D(shape) as shape, miles from transportation.all_trails", "shape", "all_trails")
    # import_and_clip("select objectid, lu15cat, lu15catn, lu15sub, lufmcat, lufmcatn, acres, state_name, co_name, mun_name, geoid, lu15dev, mixeduse, lu15subn, ST_Force2D(shape) as shape from planning.dvrpc_landuse_2015", "shape", "lu2015", gpd_kwargs={'if_exists':'replace', 'dtype': 'POLYGON' }) # mm note; this didn't work
    # import_and_clip("select objectid, verif_by, verif_on, multi_use, surface, comments_dvrpc, county, name, verif_status, owner, ST_Force2D(shape) as shape, miles from transportation.all_trails", "shape", "all_trails")
    # import_model_volumes()
    # import_adt()
    import_safety_voyager()
    # import_pavement_conditions()
    # import_jobs()
    # import_mercer_roads()
    # import_travel_times()
    # import_bridges()