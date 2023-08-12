from travel_etl.core.table import YDBTable
from travel_etl.core.table import YDBField as Field
from travel_etl.core.table import YDBIndex as Index


class DetOffersFirstTime(YDBTable):
    queries = ["prepare.sql", "query.sql"]
    params = ["days_offer", "source"]

    fields = [
        Field("hotel_id", "Int64"),
        Field("price", "Double"),
        Field("start_date", "Date"),
        Field("end_date", "Date"),
        Field("room_type", "Utf8"),
        Field("mealplan", "Utf8"),
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

    primary_keys = [
        "website",
        "hotel_id",
        "start_date",
        "end_date",
        "room_type",
        "mealplan",
    ]

    indexes = [
        Index("row_tm", ["row_extracted_dttm_utc"]),
    ]

class DetOffersFirstTimeInit(DetOffersFirstTime):
    queries = ["init.sql"]
    params = ["days_init", "source"]
