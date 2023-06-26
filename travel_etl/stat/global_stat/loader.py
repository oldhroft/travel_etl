from travel_etl.core.table import YDBTable
from travel_etl.core.table import YDBField as Field


class StatGlobalStat(YDBTable):
    params = ["days", "source"]
    queries = ["query.sql"]
    fields = [
        Field("global_id", "Utf8"),
        Field("website", "Utf8"),
        Field("utc_started_dttm", "Datetime"),
        Field("utc_ended_dttm", "Datetime"),
        Field("cnt_failed", "Int64"),
        Field("cnt_total", "Int64"),
    ]
    primary_keys = ["utc_started_dttm", "global_id"]
