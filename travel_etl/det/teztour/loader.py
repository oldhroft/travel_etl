from travel_etl.core.table import YDBTable

class DetTeztour(YDBTable):
    queries = [
         "query.sql"
    ]

    params = [
        "hours", "source"
    ]