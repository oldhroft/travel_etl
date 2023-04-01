import os
from travel_etl.utils import execute_query, initialize_pool

import logging


def load(hours, source_teztour, source_travelata, target):
    fname = os.path.join(os.path.dirname(__file__), "query.sql")

    with open(fname, "r", encoding="utf-8") as file:
        query = file.read()

    query_fmt = query % dict(
        hours=hours,
        source_teztour=source_teztour,
        source_travelata=source_travelata,
        target=target,
    )

    logging.info(f"Executing query:\n{query_fmt}")
    session = initialize_pool()
    execute_query(session, query_fmt)
