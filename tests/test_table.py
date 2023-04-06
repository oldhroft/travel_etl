import pytest

from travel_etl.core.table import Table
from travel_etl.core.utils import BasePool
import os


def test_table_creation():
    table = Table("dir", "table_name")
    assert table.table_name == "dir/table_name"
    current_path = os.path.abspath(os.path.dirname(__file__))
    parent = "/".join(current_path.split("/")[:-1])
    desired_path = os.path.join(parent, "travel_etl", "core")
    assert table.fpath == desired_path

    table = Table("dir")
    assert table.table_name == "dir/core"


def test_table_subclassing():
    class NewTable(Table):
        queries = ["query.sql"]
        params = ["param1"]

    table = NewTable("dir")
    assert table.table_name == "dir"
    current_path = os.path.abspath(os.path.dirname(__file__))

    assert table.fpath == current_path


def test_query_execution():
    queries_hist = []

    class MockPool(BasePool):
        def __init__(self) -> None:
            super().__init__()

        def execute(self, query):
            queries_hist.append(query)

    class NewTable(Table):
        pool_cls = MockPool
        params = []
        queries = ["queries/query1.sql", "queries/query2.sql"]

    table = NewTable("dir")
    table.load_table()
    assert len(queries_hist) == 2


def test_fake_etl():
    queries_hist = []

    class MockPool(BasePool):
        def __init__(self) -> None:
            super().__init__()

        def execute(self, query):
            queries_hist.append(query)

    class NewTable1(Table):
        pool_cls = MockPool
        params = []
        queries = ["queries1/query1.sql", "queries1/query2.sql"]

    class NewTable(Table):
        ...

    source = NewTable("dir")
    target = NewTable1("dir", name="table")

    target.load_table(source=source)

    assert queries_hist[0] == "query1 dir dir/table"


def test_params_check():
    class MockPool(BasePool):
        def __init__(self) -> None:
            super().__init__()

        def execute(self, query):
            ...

    class NewTable(Table):
        pool_cls = MockPool
        params = ["source"]
        queries = ["queries1/query1.sql", "queries1/query2.sql"]

    table = NewTable("dir")

    with pytest.raises(ValueError):
        table.load_table()

from travel_etl.core.table import Field, YDBField, create_table_description_ydb
import ydb

def test_field():
    field = Field(name="name", type="Int64")
    assert field.nullable

def test_ydb_field():
    field = YDBField(name="name", type="Int64")
    assert field.nullable

    assert isinstance(field.column, ydb.Column)

def test_table_description():
    fields = [
        YDBField(name="name", type="Int64"),
        YDBField(name="id", type="Int64"),
    ]
    keys = [
        "id"
    ]
    indexes = []
    desc = create_table_description_ydb(fields, keys, indexes)
    assert isinstance(desc, ydb.TableDescription)

from travel_etl.core.table import YDBIndex

def test_index():
    name = "index"
    columns = ["col1"]
    index = YDBIndex(name, columns)
    assert isinstance(index.index, ydb.TableIndex)
