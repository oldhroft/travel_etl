import os

from travel_etl.core.utils import BasePool, YDBPool
import logging

import importlib


class Table:
    queries = []
    params = []
    pool_cls = BasePool

    drop_query = "DROP TABLE {table_name}s"

    def __init__(
        self,
        directory_name,
        name=None,
    ):
        m = importlib.import_module(self.__module__)
        self.fpath = os.path.dirname(m.__file__)
        module_name = m.__name__.split(".")[1: -1]
        if name is None:
            self.table_name = "/".join([directory_name] + module_name)
        else:
            self.table_name = "/".join([directory_name, name])

    def __str__(self) -> str:
        return self.table_name

    def create_table(self):
        session = self.pool_cls()

        if session.path_exists(self.table_name):
            query = self.drop_query.format(table_name=self.table_name)
            session.ddl(query)

        query_path = os.path.join(self.fpath, "ddl.sql")
        with open(query_path, "r", encoding="utf-8") as file:
            query_text = file.read()
    
        query_fmt = query_text % {"table": self.table_name}
        logging.info(f"Executing query {query_fmt}")
        session.ddl(query_fmt)

    def load_table(self, **kwargs):
        for param in self.params:
            if param not in kwargs:
                raise ValueError(f"Missing param {param} for load_table")

        session = self.pool_cls()

        for query in self.queries:
            query_path = os.path.join(self.fpath, query)
            with open(query_path, "r", encoding="utf-8") as file:
                query_text = file.read()
            query_fmt = query_text % {"target": self.table_name, **kwargs}
            logging.info(f"Executing query {query_fmt}")
            session.execute(query_fmt)


class YDBTable(Table):
    pool_cls = YDBPool
