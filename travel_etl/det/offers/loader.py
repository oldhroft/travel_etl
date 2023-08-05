from travel_etl.core.table import YDBTable
from travel_etl.core.table import YDBField as Field
from travel_etl.core.table import YDBIndex as Index


class DetOffers(YDBTable):
    queries = ["prepare.sql", "query.sql"]
    params = ["hours", "days_offer", "source"]

    fields = [
        Field("hotel_id", "Int64"),
        Field("title", "Utf8"),
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
        Field("room_type", "Utf8"),
        Field("mealplan", "Utf8"),
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

    primary_keys = ["website", "hotel_id", "start_date", "end_date", "room_type", "mealplan"]

    indexes = [
        Index("row_tm", ["row_extracted_dttm_utc"]),
    ]
