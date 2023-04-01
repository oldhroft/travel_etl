from travel_etl.utils import initialize_pool, execute_query, create_execute_query
import ydb

query = """SELECT *
FROM `parser/prod/offers`
WHERE num_stars > 3
ORDER BY price / num_nights ASC
LIMIT 1;
"""
import os

import datetime
def serial_date_to_string(srl_no):
    new_date = datetime.datetime(1970, 1, 1, 0, 0) + datetime.timedelta(srl_no - 1)
    return new_date.strftime("%d.%m.%Y")

def initialize_session():
    driver_config = ydb.DriverConfig(
        os.environ["YDB_ENDPOINT"], os.environ["YDB_DATABASE"], 
        credentials=ydb.iam.ServiceAccountCredentials.from_file(os.environ["SA_KEY_FILE"]),
        root_certificates=ydb.load_ydb_root_certificate(),
    )
    driver =  ydb.Driver(driver_config)
    
    try:
        driver.wait(timeout=10)
        session = driver.table_client.session().create()
        return session
    except TimeoutError:
        print("Connect failed to YDB")
        print("Last reported errors by discovery:")
        print(driver.discovery_debug_details())
        exit(1)

if __name__ == "__main__":
    
    session = initialize_session()
    result = session.transaction().execute(query, commit_tx=True)[0].rows[0]

    line1 = f"На {int(result.num_nights)} ночей, с {serial_date_to_string(result.start_date)} до {serial_date_to_string(result.end_date)}\n"
    line2 = f"{result.country_name}, {result.city_name}\n"
    line3 = f"Стоимость {result.price} RUB\n"
    print(line1 + line2 + line3 + result.link)