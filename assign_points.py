from pg_data_etl import Database
import os
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

db = Database.from_config("mercer", "omad")
gis_db = Database.from_config("gis", "gis")
data_folder = Path(os.getenv("data_root"))  # path to g drive folder'


def megajoin():
    query = """
    drop schema if exists point_assignment CASCADE;
    create schema point_assignment;
    create table point_assignment.megajoin as (
        with 
        vul_crashes as(
            select a."index" , count(b.geom) as vul_crash from rejoined."all" a 
            inner join public.view_vulnerable_user_crashes b
            on st_within(b.geom, st_buffer(a.geom, 10))
            group by a.index),
        ksi as (
            select a."index" , count(b.geom) as ksi from rejoined."all" a 
            inner join public.view_ksi b
            on st_within(b.geom, st_buffer(a.geom, 10))
            group by a.index)
        select 
            a.*, 
            b.inrixxd, 
            c.unofficial_sufficiency_rating as bridge_rating,
            d.vul_crash,
            e.ksi,
            f.lsad_type
        from rejoined.all a
            left join public.bottlenecks b
                on st_within(b.geom, st_buffer(a.geom, 10))
            left join public.bridges_joined c
                on st_within(c.geom, st_buffer(a.geom, 10))
            left join vul_crashes d
                on a."index" = d."index" 
            left join ksi e
                on a."index" = e."index"
            left join public.uza f
                on st_within(a.geom, f.geom));
    """
    db.execute(query)


def create_point_cols():
    deficiencies = [
        "bridge",
        "pvmt",
        "vul_user",
        "ksi",
        "crrate",
        "sidewalk",
        "missing_bike_fac",
        "transit_rt",
        "tti",
        "pti",
        "bottleneck",
    ]
    col_names = []
    for value in deficiencies:
        v2 = value + "_pts"
        col_names.append(v2)

    for column in col_names:
        query = f"""
            alter table point_assignment.megajoin drop column if exists {column};
            alter table point_assignment.megajoin add column {column} integer;
            update point_assignment.megajoin set {column} = 0 where {column} is null;"""
        db.execute(query)


def assign_points():
    query = """
    UPDATE point_assignment.megajoin SET bridge_pts = 1 WHERE bridge_rating between 20 and 50;
    UPDATE point_assignment.megajoin SET bridge_pts = 2 WHERE bridge_rating <= 20;
    UPDATE point_assignment.megajoin SET pvmt_pts = 1 WHERE pci_new between 30 and 60;
    UPDATE point_assignment.megajoin SET pvmt_pts = 2 WHERE pci_new <= 30;
    UPDATE point_assignment.megajoin SET vul_user_pts = 2 WHERE vul_crash > 0;
    UPDATE point_assignment.megajoin SET ksi_pts = 2 WHERE ksi > 0;
    UPDATE point_assignment.megajoin SET crrate_pts = 1 WHERE crrate between 1256 and 2025;
    UPDATE point_assignment.megajoin SET crrate_pts = 2 WHERE crrate > 2025;
    UPDATE point_assignment.megajoin SET sidewalk_pts = 1 WHERE sw_ratio between .01 and .5 and lsad_type = 'Urbanized Area';
    UPDATE point_assignment.megajoin SET sidewalk_pts = 2 WHERE sw_ratio < .01 and lsad_type = 'Urbanized Area';
    UPDATE point_assignment.megajoin SET sidewalk_pts = 1 WHERE sw_ratio < .01 and lsad_type != 'Urbanized Area';
    UPDATE point_assignment.megajoin SET missing_bike_fac_pts = 1 WHERE bikefacili = 'Sharrows';
    UPDATE point_assignment.megajoin SET missing_bike_fac_pts = 2 WHERE bikefacili = 'No Accomodation';
    UPDATE point_assignment.megajoin SET tti_pts = 1 WHERE lsad_type = 'Urbanized Area' and ttiwkd0708 >= 1.5 or ttiwkd0809 >=1.5 or ttiwkd1617 >=1.5 or ttiwkd1718 >= 1.5;
    UPDATE point_assignment.megajoin SET tti_pts = 1 WHERE lsad_type != 'Urbanized Area' and ttiwkd0708 between 1.2 and 1.5 or ttiwkd0809 between 1.2 and 1.5 or ttiwkd1617 between 1.2 and 1.5 or ttiwkd1718 between 1.2 and 1.5;
    UPDATE point_assignment.megajoin SET tti_pts = 2 WHERE lsad_type != 'Urbanized Area' and ttiwkd0708 >= 1.5 or ttiwkd0809 >=1.5 or ttiwkd1617 >=1.5 or ttiwkd1718 >= 1.5;
    UPDATE point_assignment.megajoin SET pti_pts = 1 WHERE lsad_type = 'Urbanized Area' and ptiwkd0708 >= 3 or ptiwkd0809 >=3 or ptiwkd1617 >=3 or ptiwkd1718 >=3;
    UPDATE point_assignment.megajoin SET pti_pts = 1 WHERE lsad_type != 'Urbanized Area' and ptiwkd0708 between 2 and 3 or ptiwkd0809 between 2 and 3 or ptiwkd1617 between 2 and 3 or ptiwkd1718 between 2 and 3;
    UPDATE point_assignment.megajoin SET pti_pts = 2 WHERE lsad_type != 'Urbanized Area' and ptiwkd0708 >= 3 or ptiwkd0809 >=3 or ptiwkd1617 >=3 or ptiwkd1718 >=3;
    UPDATE point_assignment.megajoin SET bottleneck_pts = 1 WHERE inrixxd=0;
    UPDATE point_assignment.megajoin SET transit_rt_pts = 1 WHERE line is not null;
    UPDATE point_assignment.megajoin SET transit_rt_pts = 2 WHERE busfreq >=3 or busfreq2 >=3;  
    drop table if exists point_assignment.total_points;
    create table point_assignment.total_points as 
    select *, bridge_pts + pvmt_pts + vul_user_pts + ksi_pts + crrate_pts + sidewalk_pts + missing_bike_fac_pts + transit_rt_pts + tti_pts + pti_pts + bottleneck_pts as total from point_assignment.megajoin;
    """
    db.execute(query)


def critical_flag():
    query = """
        alter table point_assignment.total_points add column critical int;
        UPDATE point_assignment.total_points SET critical = 1 WHERE bridge_rating <= 20;"""
    db.execute(query)


if __name__ == "__main__":
    megajoin()
    create_point_cols()
    assign_points()
    critical_flag()
