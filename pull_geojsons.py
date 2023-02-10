from pg_data_etl import Database
import os
from dotenv import load_dotenv
from pathlib import Path
from assign_pts import scenarios

load_dotenv()

db = Database.from_config("mercer", "omad")
data_folder = Path(os.getenv("data_root"))  # path to g drive folder'
outputpath = data_folder / "Outputs" / "GeoJSON"


def create_and_export_geojson(scenario: str):
    os.system(
        f"""ogr2ogr -f GeoJSON '{outputpath}'/scenario_{scenario}.geojson  -lco -overwrite PG:"host={db.connection_params['host']} port={db.connection_params['port']} user={db.connection_params['un']} dbname={db.connection_params['db_name']} password={db.connection_params['pw']}" -sql "select * from point_assignment.scenario_{scenario}" 
                """
    )


if __name__ == "__main__":
    for scenario in scenarios:
        print(f"working on scenario_{scenario}")
        create_and_export_geojson(scenario)
