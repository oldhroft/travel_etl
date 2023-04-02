from travel_etl.core.table import YDBTable

class DetTravelata(YDBTable):
    queries = [
         "query.sql"
    ]

    params = [
        "hours", "source"
    ]