from pg_data_etl import Database
import os
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

db = Database.from_config("mercer", "omad")
gis_db = Database.from_config("gis", "gis")
data_folder = Path(os.getenv("data_root"))  # path to g drive folder'


def remove_bottlenecks():
    query = """
        drop table if exists point_assignment.no_bottlenecks;
        create table point_assignment.no_bottlenecks as
	        select * from point_assignment.total_points;
        update point_assignment.no_bottlenecks
        set total = total - bottleneck_pts;
    """
    db.execute(query)


if __name__ == "__main__":
    remove_bottlenecks()
