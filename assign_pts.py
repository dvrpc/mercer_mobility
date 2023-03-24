from pg_data_etl import Database
from dotenv import load_dotenv
import os

load_dotenv()
api_key = os.getenv("api_key")

db = Database.from_config("mercer", "omad")


def rank_crashes():
    """Rank segments by different types of crash rates into 5 bins,
    from bottom 20% to top 20%"""

    rates = ["crrate", "ksicrrate", "vulcrrate"]
    for rate in rates:
        query = f"""
            alter table rejoined.all add column if not exists {rate}_pts int;

            --0-20%
            update rejoined.all
            set {rate}_pts = 1 where {rate} between 0 and (select max({rate})/5 from rejoined.all where countyrd = 'County');

            --21-40%
            update rejoined.all
            set {rate}_pts = 2 where {rate} between (select max({rate})/5 from rejoined.all where countyrd = 'County') and ((select max({rate})/5 from rejoined.all where countyrd = 'County') * 2);

            --41-60%
            update rejoined.all
            set {rate}_pts = 3 where {rate} between ((select max({rate})/5 from rejoined.all where countyrd = 'County') * 2) and ((select max({rate})/5 from rejoined.all where countyrd = 'County') * 3);

            --60-80%
            update rejoined.all
            set {rate}_pts = 4 where {rate} between ((select max({rate})/5 from rejoined.all where countyrd = 'County') * 3) and  ((select max({rate})/5 from rejoined.all where countyrd = 'County') * 4);


            --80-100%
            update rejoined.all
            set {rate}_pts = 5 where {rate} between  ((select max({rate})/5 from rejoined.all where countyrd = 'County') * 4) and ((select max({rate})/5 from rejoined.all where countyrd = 'County') * 5);
            """
        db.execute(query)

    query = """alter table rejoined.all add column if not exists crash_pt_totals int;
                update rejoined.all set crash_pt_totals = crrate_pts + ksicrrate_pts + vulcrrate_pts; """

    db.execute(query)


def high_priority():
    query = """
    alter table rejoined.all add column if not exists high_density_equity bool;

    update rejoined.all
    set high_density_equity = 'True' 
    from high_priority
    where st_within(rejoined.all.geom, high_priority.geom);

    update rejoined.all
    set high_density_equity = 'False'
    where high_density_equity is null;
    """
    db.execute(query)


def overlap():
    """Checks for overlap in highest scoring congestion segments and highest scoring crash segments.

    Congestion is easy because they're ranked, crashes use the crash_pt_totals column
    and only greater than 9 (which is the start of the top 20% of a max of 11 pts)"""

    query = """
    alter table rejoined.all add column if not exists crash_congestion_overlap bool;

    update rejoined.all
    set crash_congestion_overlap = 'True'
    where (rankvehdel <= 20 or rankvoldel <= 20) and (crash_pt_totals >= 9);

    update rejoined.all
    set crash_congestion_overlap = 'False'
    where crash_congestion_overlap is null;
    """

    db.execute(query)


if __name__ == "__main__":
    rank_crashes()
    high_priority()
    overlap()
