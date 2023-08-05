import ydb
import ydb.iam
import os

from typing import Callable


def create_execute_query(query: str) -> Callable:

    TIMEOUT = 30
    # Create the transaction and execute query.
    def _execute_query(session: ydb.SessionPool):
        session.transaction().execute(
            query,
            commit_tx=True,
            settings=ydb.BaseRequestSettings()
            .with_timeout(TIMEOUT + 5)
            .with_operation_timeout(TIMEOUT),
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

    def execute_load(self, query):...

    def path_exists(self, path):
        return False
    
    def ddl(self, description, table_name):...


class YDBPool(BasePool):
    def __init__(self) -> None:
        self.driver = initialize_driver()
        self.session = ydb.SessionPool(self.driver)
        self.table_session = self.driver.table_client.session().create()

    def execute(self, query):
        execute_query(self.session, query)

    def drop_table(self, table_name):
        self.table_session.drop_table(os.path.join(os.environ["YDB_DATABASE"], table_name))

    def ddl(self, description, table_name, drop_if_exists=True):
        if drop_if_exists and self.path_exists(table_name):
            self.drop_table(table_name)

        self.table_session.create_table(
            os.path.join(os.environ["YDB_DATABASE"], table_name),
            description
        )

    def path_exists(self, path):
        try:
            full_path = os.path.join(os.environ["YDB_DATABASE"], path)
            self.driver.scheme_client.describe_path(full_path)
            return True
        except ydb.SchemeError as e:
            return False
        
    def execute_load(self, query: str) -> dict:
        results = self.table_session.transaction().execute(query, commit_tx=True)[0].rows
        return list(map(dict, results))

from typing import Union
import json
import boto3


def load_to_s3(s3_client: boto3.Session.client, 
               data: Union[str, dict, list], Key: str, Bucket: str) -> None:
    
    if isinstance(data, list) or isinstance(data, dict):
        data = json.dumps(data)
    elif isinstance(data, str):
        pass
    else:
        raise ValueError("data should be str, list or dict")
    s3_client.put_object(Body=data, Bucket=Bucket, Key=Key)

class S3Client:

    def __init__(self) -> None:
        self.session = boto3.session.Session(
            aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        )

        self.s3_client = self.session.client(
            service_name="s3",
            endpoint_url=os.environ["AWS_ENDPOITNT_URL"],
            region_name=os.environ["AWS_REGION_NAME"],
        )
    
    def load_to_s3(self, data: Union[str, dict, list], Key: str, Bucket: str) -> None:
        load_to_s3(self.s3_client, data, Key, Bucket)
