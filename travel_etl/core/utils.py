import ydb
import ydb.iam
import os

from typing import Callable


def create_execute_query(query: str) -> Callable:
    # Create the transaction and execute query.
    def _execute_query(session: ydb.SessionPool):
        session.transaction().execute(
            query,
            commit_tx=True,
            settings=ydb.BaseRequestSettings()
            .with_timeout(20)
            .with_operation_timeout(19),
        )

    return _execute_query


def execute_query(session: ydb.SessionPool, query: str) -> None:
    session.retry_operation_sync(create_execute_query(query))


def initialize_pool() -> ydb.Driver:
    driver_config = ydb.DriverConfig(
        os.environ["YDB_ENDPOINT"],
        os.environ["YDB_DATABASE"],
        credentials=ydb.iam.ServiceAccountCredentials.from_file(
            os.environ["SA_KEY_FILE"]
        ),
        root_certificates=ydb.load_ydb_root_certificate(),
    )
    driver = ydb.Driver(driver_config)

    try:
        driver.wait(timeout=10)
        pool = ydb.SessionPool(driver)
        return pool
    except TimeoutError:
        print("Connect failed to YDB")
        print("Last reported errors by discovery:")
        print(driver.discovery_debug_details())
        exit(1)


class BasePool:
    def __init__(self) -> None:
        pass


class YDBPool(BasePool):
    def __init__(self) -> None:
        self.session = initialize_pool()

    def execute(self, query):
        execute_query(self, query)
