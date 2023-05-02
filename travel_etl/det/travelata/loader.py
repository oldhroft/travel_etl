from travel_etl.core.table import YDBTable
from travel_etl.core.table import YDBField as Field

class DetTravelata(YDBTable):
    queries = [
         "query.sql"
    ]

    params = [
        "hours", "source"
    ]

    fields = [
        Field("title", "Utf8"),
        Field("num_stars", "Int64"),
        Field("flag_less_places", "Bool"),
        Field("location_name", "Utf8"),
        Field("country_name", "Utf8"),
        Field("rating", "Double"),
        Field("num_reviews", "Int64"),
        Field("orders_count", "Int64"),
        Field("oil_tax_value", "Double"),
        Field("hotel_id", "Int64"),
        Field("price", "Double"),
        Field("num_people", "Int64"),
        Field("start_date", "Date"),
        Field("end_date", "Date"),
        Field("airport_distance", "Double"),
        Field("beach_distance", "Double"),
        Field("wifi_option", "Utf8"),
        Field("condi_option", "Utf8"),
        Field("num_nights", "Int64"),
        Field("is_flight_included", "Bool"),
        Field("beach_line", "Int64"),
        Field("sand_beach_flg", "Bool"),
        Field("pebble_beach_flg", "Bool"),
        Field("beach_options", "Utf8"),
        Field("link", "Utf8"),
        Field("website", "Utf8"),
        Field("offer_hash", "Utf8"),
        Field("key", "Utf8"),
        Field("bucket", "Utf8"),
        Field("parsing_id", "Utf8"),
        Field("row_id", "Utf8"),
        Field("row_extracted_dttm_utc", "Datetime"),
        Field("created_dttm_utc", "Datetime")
    ]
    
    primary_keys = ["row_extracted_dttm_utc", "parsing_id", "row_id"]