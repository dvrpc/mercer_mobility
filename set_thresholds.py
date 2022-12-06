from pg_data_etl import Database

db = Database.from_config("mercer", "omad")

views = []


def create_threshold_view(
    viewname: str,
    table: str,
    column: str,
    sign: str,
    threshold,
    and_or_statement: str = "",
    # append_to_views: bool = True,
):
    """creates a view in postgres based on a threshold

    :param viewname: what you'd like the view to be called
    :param table: tablename you're selecting from
    :param column: column you're using for a threshold
    :param sign: > < or =
    :param threshold: threshold you want to set for a layer
    :param and_or_statement: if you want to add conditional and/or statements to base query
    :param append_to_views: only change if you want to later clip to road buffer (vs joining based on columns)"""
    db.execute(
        f"""create or replace view view_{viewname} as(
                    select * from {table}  
                    where {column} {sign} {threshold}
                    {and_or_statement}
                    );"""
    )
    views.append("view_" + viewname)


def clip_to_mercerroads(view, line_buffer: float):
    """clips segments in "views list" to a small buffer around mercer roads"""
    db.execute(
        f"""drop table if exists {view}_clipped;
            create table {view}_clipped as(
            with buffered as (
                select st_buffer(geom, {line_buffer}) as geom from mercercountyjurisdictionroads_frommercer
                )
                select a.* from public.{view} a 
                inner join buffered b 
                on st_intersects(a.geom, b.geom)
                )"""
    )
    print(f"creating clipped table for {view} view...")


def set_thresholds():
    for val in [50, 80]:
        create_threshold_view(
            f"bridges{val}", "bridges_joined", "unofficial_sufficiency_rating", "<", val
        )

    for val in [40, 70]:
        create_threshold_view(f"pavement{val}", "pavement_evaluation", "pci", "<", val)

    for val in [85, 100]:
        for period in ["model_vols2025_am_link", "model_vols2025_pm_link"]:
            create_threshold_view(
                f"{period[-7:-5]}_vc{val}",
                f"{period}",
                '"volcapra~2"::int',
                ">",
                val,
            )

    create_threshold_view("sw_gaps", "sidewalk_gaps_clipped", "sw_ratio", "<", ".5")

    create_threshold_view(
        "vulnerable_user_crashes",
        "safety_voyager",
        "pedestrian_involved",
        ">",
        0,
        "or cyclist_involved > 0",
    )

    create_threshold_view(
        "ksi",
        "safety_voyager",
        "severity_rating_code",
        "=",
        "'Fatal Injury'",
        "or severity_rating_code = 'Suspected Serious Injury'",
    )

    create_threshold_view(
        "crashrate_seg",
        "crash_statistics_by_segment_mc",
        "crrate",
        ">",
        482,
        # append_to_views=False,
    )

    create_threshold_view(
        "ksi_rate_seg",
        "crash_statistics_by_segment_mc",
        "ksicrrate",
        ">",
        2.5,
        # append_to_views=False,
    )

    for timeperiod in ["0809", "0910", "1617", "1718"]:
        create_threshold_view(
            f"tti_{timeperiod}",
            "dvrpcnj_inrixxdgeo22_1_jointraveltime1min_mercer",
            f"ttiwkd{timeperiod}",
            ">=",
            1.2,
            f"and ttiwkd{timeperiod} <= 1.5",
        )

    for timeperiod in ["0809", "0910", "1617", "1718"]:
        create_threshold_view(
            f"pti_{timeperiod}",
            "dvrpcnj_inrixxdgeo22_1_jointraveltime1min_mercer",
            f"ptiwkd{timeperiod}",
            ">=",
            2,
            f"and ptiwkd{timeperiod} <= 3",
        )
    print("thresholds set")


set_thresholds()
# for view in views:
#     clip_to_mercerroads(view, 15.24)  # 15.24 is meters, == 50'
