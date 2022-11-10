from pg_data_etl import Database

db = Database.from_config("mercer", "omad")


def create_threshold_view(
    viewname: str,
    table: str,
    column: str,
    sign: str,
    threshold,
    and_or_statement: str = "",
):
    """creates a view in postgres based on a threshold"""
    db.execute(
        f"""create or replace view view_{viewname} as(
                    select * from {table}  
                    where {column} {sign} {threshold}
                    {and_or_statement}
                    );"""
    )


if __name__ == "__main__":

    for val in [50, 80]:
        create_threshold_view(
            f"bridges{val}", "bridges_joined", "unofficial_sufficiency_rating", "<", val
        )

    for val in [40, 70]:
        create_threshold_view(f"pavement{val}", "pavement_evaluation", "pci", "<", val)

    for val in [85, 100]:
        create_threshold_view(
            f"vc{val}",
            "model_vol_2025_am_link",
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
