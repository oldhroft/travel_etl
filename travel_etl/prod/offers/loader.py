import os
from travel_etl.utils import execute_query, initialize_pool

import logging

def load(hours, days_offer, source, target):
    fname = os.path.join(os.path.dirname(__file__), "query.sql")
    fname_prepare = os.path.join(os.path.dirname(__file__), "prepare.sql")

    with open(fname, "r", encoding="utf-8") as file:
        query = file.read()
    
    with open(fname_prepare, "r", encoding="utf-8") as file:
        query_prep = file.read()

    query_fmt = query % dict(hours=hours, source=source, target=target,)
    query_prep_fmt = query_prep % dict(days_offer=days_offer, target=target,)
    
    session = initialize_pool()

    logging.info(f"Executing query:\n{query_prep_fmt}")
    execute_query(session, query_prep_fmt)
    logging.info(f"Executing query:\n{query_fmt}")
    execute_query(session, query_fmt)
