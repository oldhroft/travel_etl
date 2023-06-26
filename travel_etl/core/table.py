import os

from travel_etl.core.utils import BasePool, YDBPool, S3Client
import logging

import importlib
import ydb

import datetime


class DataObject:
    dttm_formatter = lambda x: datetime.datetime.strftime(x, "%Y-%m-%d %H:%M:%S")

    def __init__(
        self,
        directory_name,
        name=None,
    ):
        m = importlib.import_module(self.__module__)
        self.fpath = os.path.dirname(m.__file__)
        module_name = m.__name__.split(".")[1:-1]
        if name is None:
            self.table_name = "/".join([directory_name] + module_name)
        else:
            self.table_name = "/".join([directory_name, name])

        dttm = datetime.datetime.now()
        self.dttm = self.dttm_formatter(dttm)

    def __str__(self) -> str:
        return self.table_name


class Table(DataObject):
    queries = []
    params = []
    pool_cls = BasePool
    fields = []

    def create_table_description(self):
        self.description = None

    def __init__(self, directory_name, name=None):
        super().__init__(directory_name, name)
        self.create_table_description()

    def create_table(self):
        session = self.pool_cls()
        session.ddl(self.description, self.table_name)

    def load_table(self, **kwargs):
        for param in self.params:
            if param not in kwargs:
                raise ValueError(f"Missing param {param} for load_table")

        session = self.pool_cls()

        for query in self.queries:
            query_path = os.path.join(self.fpath, query)
            with open(query_path, "r", encoding="utf-8") as file:
                query_text = file.read()
            query_fmt = query_text % {
                "target": self.table_name,
                "dttm": self.dttm,
                **kwargs,
            }
            logging.info(f"Executing query {query_fmt}")
            session.execute(query_fmt)


class Field:
    def __init__(self, name, type, nullable=True):
        self.name = name
        self.type = type
        self.nullable = nullable


class YDBField(Field):
    def __init__(self, name, type, nullable=True):
        super().__init__(name, type, nullable)
        base_type = getattr(ydb.PrimitiveType, type)
        if nullable:
            base_type = ydb.OptionalType(base_type)

        self.column = ydb.Column(name, type=base_type)


class YDBIndex:
    def __init__(self, name, columns):
        self.index = ydb.TableIndex(name).with_index_columns(*columns)


from typing import List, Optional


def create_table_description_ydb(
    fields: List[YDBField],
    primary_keys: list,
    indexes: List[YDBIndex],
    ttl_settings: Optional[tuple],
):
    builder = ydb.TableDescription()
    builder = builder.with_columns(*[field.column for field in fields])
    builder.with_primary_keys(*primary_keys)
    builder.with_indexes(*[index.index for index in indexes])

    if ttl_settings is not None:
        # This is untested
        column, ttl = ttl_settings
        builder = builder.with_ttl(
            ydb.TtlSettings().with_date_type_column(
                column_name=column, expire_after_seconds=ttl
            )
        )

    return builder


def format_dttm_ydb(dttm: datetime.datetime) -> str:
    dttm_fmt = dttm.strftime("%Y-%m-%dT%H:%M:%SZ")
    return f"cast({dttm_fmt} as datetime)"


class YDBTable(Table):
    pool_cls = YDBPool
    fields = []
    primary_keys = []
    indexes = []
    ttl_settings = None
    dttm_formatter = format_dttm_ydb

    def create_table_description(self):
        self.description = create_table_description_ydb(
            self.fields, self.primary_keys, self.indexes, ttl_settings=self.ttl_settings
        )


class S3Json(DataObject):
    pool_cls = BasePool

    s3_session = S3Client

    params = []
    queries = []

    def __init__(self, directory_name, Bucket, name=None):
        super().__init__(directory_name, name)
        self.Bucket = Bucket
        self.table_name = self.table_name + ".json"

    def transform(self):
        self.result = []

    def load(self, **kwargs):
        for param in self.params:
            if param not in kwargs:
                raise ValueError(f"Missing param {param} for load_table")

        session = self.pool_cls()

        self.data = []

        for query in self.queries:
            query_path = os.path.join(self.fpath, query)
            with open(query_path, "r", encoding="utf-8") as file:
                query_text = file.read()
            query_fmt = query_text % {
                "target": self.table_name,
                "dttm": self.dttm,
                **kwargs,
            }
            logging.info(f"Executing query {query_fmt}")
            res = session.execute_load(query_fmt)
            self.data.append(res)

        self.transform()
        s3 = self.s3_session()

        s3.load_to_s3(self.result, Key=self.table_name, Bucket=self.Bucket)


class S3JsonFromYDB(S3Json):
    pool_cls = YDBPool
    dttm_formatter = format_dttm_ydb
