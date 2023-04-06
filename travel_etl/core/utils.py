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


def initialize_driver() -> ydb.Driver:
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

        return driver
    except TimeoutError:
        print("Connect failed to YDB")
        print("Last reported errors by discovery:")
        print(driver.discovery_debug_details())
        exit(1)


class BasePool:
    def __init__(self) -> None:
        pass

    def execute(self, query):...

    def path_exists(self, path):
        return False


class YDBPool(BasePool):
    def __init__(self) -> None:
        self.driver = initialize_driver()
        self.session = ydb.SessionPool(self.driver)
        self.table_session = self.driver.table_client.session().create()

    def execute(self, query):
        execute_query(self.session, query)

    def ddl(self, query):
        self.table_session.transaction().execute(query)

    def paths_exists(self, path):
        try:
            full_path = os.path.join(os.environ["YDB_DATABASE"], path)
            self.driver.scheme_client.describe_path(full_path)
            return True
        except ydb.SchemeError as e:
            return False
        
