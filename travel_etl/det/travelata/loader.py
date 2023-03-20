import os
from travel_etl.utils import execute_query, initialize_pool

def load():
    fname = os.path.join(os.path.dirname(__file__), "query.sql")
    with open(fname, "r", encoding="utf-8") as file:
        query = file.read()
        
    session = initialize_pool()
    execute_query(session, query)