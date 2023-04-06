from travel_etl.core.table import YDBTable

class DetPivot(YDBTable):
    queries = [
        "query.sql",
    ]

    params = [
        "hours", "source_teztour", "source_travelata"
    ]
