from pg_data_etl import Database
import os
from dotenv import load_dotenv
from pathlib import Path
import geopandas as gpd

load_dotenv()

db = Database.from_config("mercer", "omad")
gis_db = Database.from_config("gis", "gis")
data_folder = Path(os.getenv("data_root"))  # path to g drive folder'


def create_and_export_geojson(column: str, points_column: str, where_statement: str):
    query = f"""
        select {column}, {points_column}, geom
        from point_assignment.total_points
        where {where_statement} 
    """
    gdf = db.gdf(query)
    gdf.to_file(data_folder / "Outputs" / "geoJSON" / f"{points_column}.geojson")


if __name__ == "__main__":
    create_and_export_geojson("bridge_rating", "bridge_pts", "bridge_rating <= 50")
    create_and_export_geojson("pci_new", "pvmt_pts", "pci_new <= 60")
    create_and_export_geojson("vul_crash", "vul_user_pts", "vul_crash > 0")
    create_and_export_geojson("ksi", "ksi_pts", "ksi > 0")
    create_and_export_geojson("crrate", "crrate_pts", "crrate > 1256")
    create_and_export_geojson("sw_ratio", "sidewalk_pts", "sw_ratio > 0")
    create_and_export_geojson(
        "bikefacili", "missing_bike_fac_pts", "bikefacili = 'No Accomodation'"
    )
    create_and_export_geojson("inrixxd", "bottleneck_pts", "inrixxd=0")
    create_and_export_geojson("line", "transit_rt_pts", "line is not null")
    create_and_export_geojson(
        "ptiwkd0708, ptiwkd0809, ptiwkd1617, ptiwkd1718",
        "pti_pts",
        "ptiwkd0708 >= 3 or ptiwkd0809 >=3 or ptiwkd1617 >=3 or ptiwkd1718 >=3",
    )
    create_and_export_geojson(
        "ttiwkd0708, ttiwkd0809, ttiwkd1617, ttiwkd1718",
        "tti_pts",
        "ttiwkd0708 >= 1.5 or ttiwkd0809 >=1.5 or ttiwkd1617 >=1.5 or ttiwkd1718 >= 1.5",
    )
    create_and_export_geojson(
        "sri,amvc100,amvc85,crrate,bikefacili,countyrd,line,pci_new,pmvc100,pmvc85,ptiwkd0708,ptiwkd0809,ptiwkd1617,ptiwkd1718,ttiwkd0708,ttiwkd0809,ttiwkd1617,ttiwkd1718,sw_ratio,busfreq,busfreq2,inrixxd,bridge_rating,vul_crash,ksi,lsad_type,bridge_pts,pvmt_pts,vul_user_pts,ksi_pts,crrate_pts,sidewalk_pts,missing_bike_fac_pts,transit_rt_pts,tti_pts,pti_pts,bottleneck_pts,critical",
        "total",
        "total >= 0",
    )
