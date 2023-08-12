from travel_etl.det.pivot import DetPivot
from travel_etl.det.travelata import DetTravelata
from travel_etl.det.teztour import DetTeztour
from travel_etl.det.offers import DetOffers
from travel_etl.det.offers_first_time import DetOffersFirstTime
from travel_etl.prod.offers import ProdOffers
from travel_etl.stat.global_stat import StatGlobalStat

import logging


# TODO: create deploy config

def create_database(base_dir: str) -> None:
    tables = []
    # tables.append(DetTravelata(base_dir))
    # tables.append(DetTeztour(base_dir))
    # tables.append(DetPivot(base_dir))
    # tables.append(DetOffers(base_dir))
    # tables.append(ProdOffers(base_dir))
    # tables.append(DetOffersFirstTime(base_dir))
    # tables.append(StatGlobalStat(base_dir))
    for table in tables:
        print(f"Creating table {table}")
        table.create_table()
