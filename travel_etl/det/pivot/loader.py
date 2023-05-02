from travel_etl.core.table import YDBTable
from travel_etl.core.table import YDBField as Field


class DetPivot(YDBTable):
    queries = [
        "query.sql",
    ]

    params = ["hours", "source_teztour", "source_travelata"]

    fields = [
        Field("title", "Utf8"),
        Field("hotel_id", "Int64"),
        Field("country_name", "Utf8"),
        Field("city_name", "Utf8"),
        Field("price", "Double"),
        Field("airport_distance", "Double"),
        Field("sand_beach_flg", "Bool"),
        Field("start_date", "Date"),
        Field("end_date", "Date"),
        Field("rating", "Double"),
        Field("num_nights", "Double"),
        Field("is_flight_included", "Bool"),
        Field("beach_line", "Int64"),
        Field("num_stars", "Double"),
        Field("link", "Utf8"),
        Field("website", "Utf8"),
        Field("offer_hash", "Utf8"),
        Field("key", "Utf8"),
        Field("bucket", "Utf8"),
        Field("parsing_id", "Utf8"),
        Field("row_id", "Utf8"),
        Field("row_extracted_dttm_utc", "Datetime"),
        Field("created_dttm_utc", "Datetime"),
    ]
    primary_keys = ["row_extracted_dttm_utc", "parsing_id", "row_id"]
