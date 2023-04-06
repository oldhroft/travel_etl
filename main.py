from travel_etl.core.table import YDBTable
import ydb

if __name__ == "__main__":

    table = YDBTable("test")
    table.create_table()