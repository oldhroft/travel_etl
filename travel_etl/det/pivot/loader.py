from travel_etl.core.table import YDBTable

class DetPivot(YDBTable):
    queries = [
        "prepare.sql", "query.sql"
    ]

    params = [
        "hours", "source_teztour", "source_travelata"
    ]
