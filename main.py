from travel_etl.prod.offers import ProdOffers

if __name__ == "__main__":
    table = ProdOffers("test")
    table.create_table()
