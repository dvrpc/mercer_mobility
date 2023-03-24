from pg_data_etl import Database
import os
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

db = Database.from_config("mercer", "omad")
data_folder = Path(os.getenv("data_root"))  # path to g drive folder'
outputpath = data_folder / "Outputs" / "GeoJSON"


def create_and_export_geojson(
    schema: str,
    pg_tablename: str,
):
    os.system(
        f"ogr2ogr -f GeoJSON '{outputpath}'/{pg_tablename}.geojson "
        + f"-lco -overwrite PG:'host={db.connection_params['host']} "
        + f"port={db.connection_params['port']} "
        + f"user={db.connection_params['un']} "
        + f"dbname={db.connection_params['db_name']} "
        + f"password={db.connection_params['pw']}' -sql 'select * from {schema}.{pg_tablename}'"
    )


if __name__ == "__main__":
    create_and_export_geojson("rejoined", "all")
    create_and_export_geojson("public", "high_priority")
