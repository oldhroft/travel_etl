from travel_etl.det.pivot import DetPivot
from travel_etl.det.travelata import DetTravelata
from travel_etl.det.teztour import DetTeztour
from travel_etl.prod.offers import ProdOffers
from travel_etl.stat.global_stat import StatGlobalStat


import logging


def create_database(base_dir: str) -> None:
    tables = []
    tables.append(DetTravelata(base_dir))
    tables.append(DetTeztour(base_dir))
    tables.append(DetPivot(base_dir))
    tables.append(ProdOffers(base_dir))
    tables.append(StatGlobalStat(base_dir))
    for table in tables:
        print(f"Creating table {table}")
        table.create_table()
