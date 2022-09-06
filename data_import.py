from pg_data_etl import Database
db = Database.from_config("mercer", "localhost")
gis_db = Database.from_config("gis", "gis")

gdf = gis_db.gdf("select * from transportation.njdot_lrs", geom_col= "shape")
db.import_geodataframe(gdf, "lrs", )

