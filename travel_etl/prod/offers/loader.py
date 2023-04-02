from travel_etl.core.table import YDBTable


class ProdOffers(YDBTable):
    queries = ["prepare.sql", "query.sql"]
    params = ["hours", "days_offer", "source"]
