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
    """creates a view in postgres based on a threshold, to reduce the number of features you have to conflate later. basically setting a floor.

        this was not done for all layers, only those where setting a floor is acceptable. other layers needed all features to be conflated.

    :param viewname: what you'd like the view to be called
    :param table: tablename you're selecting from
    :param column: column you're using for a threshold
    :param sign: >=< or =
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


def set_thresholds():
    # model data
    for val in [85, 100]:
        for period in ["model_vols2025_am_link", "model_vols2025_pm_link"]:
            create_threshold_view(
                f"{period[-7:-5]}_vc{val}",
                f"{period}",
                '"volcapra~2"::int',
                ">",
                val,
            )

    # pti/tti
    create_threshold_view(
        f"tti_all",
        "dvrpcnj_inrixxdgeo22_1_jointraveltime1min_mercer",
        "ttiwkd0708",
        ">=",
        1.2,
        "or ttiwkd0809 >=1.2 or ttiwkd1617 >=1.2 or ttiwkd1718 >=1.2",
    )

    create_threshold_view(
        f"pti_all",
        "dvrpcnj_inrixxdgeo22_1_jointraveltime1min_mercer",
        f"ptiwkd0708",
        ">=",
        2,
        "or ptiwkd0809 >=2 or ptiwkd1617 >=2 or ptiwkd1718 >=2",
    )

    print("thresholds set")


set_thresholds()
