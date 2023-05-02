from travel_etl.core.table import YDBTable
from travel_etl.core.table import YDBField as Field

class DetTeztour(YDBTable):
    queries = [
         "query.sql"
    ]

    params = [
        "hours", "source"
    ]

    fields = [
        Field("hotel_id", "Int64"),
        Field("rating", "Double"),
        Field("currency_code", "Double"),
        Field("latitude", "Double"),
        Field("longitude", "Double"),
        Field("price", "Double"),
        Field("num_stars", "Double"),
        Field("country_name", "Utf8"),
        Field("city_name", "Utf8"),
        Field("is_flight_included", "Bool"),
        Field("room_type", "Utf8"),
        Field("mealplan", "Utf8"),
        Field("preview_img", "Utf8"),
        Field("title", "Utf8"),
        Field("price_dollars", "Double"),
        Field("price_euros", "Double"),
        Field("location_name", "Utf8"),
        Field("beach_line", "Int64"),
        Field("is_free_internet", "Bool"),
        Field("airport_distance", "Double"),
        Field("num_nights", "Int64"),
        Field("start_date", "Date"),
        Field("end_date", "Date"),
        Field("sand_beach_flg", "Bool"),
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