import datetime
import yaml
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.decorators import task

import os

# DO NOT CHNAGE, INITIALIZATION
PROJECT_PATH = os.path.dirname(__file__)
CONFIG_PATH = os.path.join(PROJECT_PATH, "config.yaml")

with open(CONFIG_PATH, "r", encoding="utf-8") as file:
    CONFIG = yaml.safe_load(file)

os.chdir(os.environ["AIRFLOW_HOME"])
PATH_TO_PYTHON = CONFIG["system"]["path_to_python"]
# DO NOT CHNAGE, INITIALIZATION


HOURS = CONFIG["HOURS"]
DAYS_OFFER = CONFIG["DAYS_OFFER"]
DIRECTORY = CONFIG["DIRECTORY"]
BUCKET = CONFIG["BUCKET"]
SCHEDULE = CONFIG["SCHEDULE"]
DAYS_STAT = CONFIG["DAYS_STAT"]
SCHEDULE_STAT = CONFIG["SCHEDULE_STAT"]
DAYS_OFFER_FIRST_TIME = CONFIG["DAYS_OFFER_FIRST_TIME"]


@task.external_python(task_id="etl_det_travelata", python=PATH_TO_PYTHON)
def etl_det_travelata(hours, directory):
    import travel_etl.det.travelata as travelata

    cfg = {
        "source": "parser/raw/travelata",
        "hours": hours,
    }

    det_travelata = travelata.DetTravelata(directory)
    det_travelata.load_table(**cfg)


@task.external_python(task_id="etl_det_teztour", python=PATH_TO_PYTHON)
def etl_det_teztour(hours, directory):
    import travel_etl.det.teztour as teztour

    cfg = {
        "source": "parser/raw/teztour",
        "hours": hours,
    }

    det_teztour = teztour.DetTeztour(directory)
    det_teztour.load_table(**cfg)


@task.external_python(task_id="etl_det_pivot", python=PATH_TO_PYTHON)
def etl_det_pivot(hours, directory):
    import travel_etl.det.teztour as teztour
    import travel_etl.det.travelata as travelata
    import travel_etl.det.pivot as pivot

    det_teztour = teztour.DetTeztour(directory)
    det_travelata = travelata.DetTravelata(directory)
    det_pivot = pivot.DetPivot(directory)

    cfg = {
        "source_teztour": det_teztour,
        "source_travelata": det_travelata,
        "hours": hours,
    }

    det_pivot.load_table(**cfg)


@task.external_python(task_id="etl_det_offers", python=PATH_TO_PYTHON)
def etl_det_offers(hours, directory, days_offer):
    import travel_etl.det.offers as offers
    import travel_etl.det.pivot as pivot

    det_pivot = pivot.DetPivot(directory)
    det_offers = offers.DetOffers(directory)

    cfg = {
        "source": det_pivot,
        "hours": hours,
        "days_offer": days_offer,
    }

    det_offers.load_table(**cfg)


@task.external_python(task_id="etl_det_offers_first_time", python=PATH_TO_PYTHON)
def etl_det_offers_first_time(directory, days_offer):
    import travel_etl.det.offers as offers
    import travel_etl.det.offers_first_time as offers_first_time

    det_offers = offers.DetOffers(directory)
    det_offers_first_time = offers_first_time.DetOffersFirstTime(directory)

    cfg = {
        "source": det_offers,
        "days_offer": days_offer,
    }

    det_offers_first_time.load_table(**cfg)


@task.external_python(task_id="etl_prod_offers", python=PATH_TO_PYTHON)
def etl_prod_offers(directory, days_offer):
    import travel_etl.det.offers as offers
    import travel_etl.det.offers_first_time as offers_first_time
    import travel_etl.prod.offers as prod_offers

    det_offers = offers.DetOffers(directory)
    det_offers_first_time = offers_first_time.DetOffersFirstTime(directory)
    prod_offers = prod_offers.ProdOffers(directory)

    cfg = {
        "source": det_offers,
        "source_first_time": det_offers_first_time,
        "days_offer": days_offer,
    }

    prod_offers.load_table(**cfg)


@task.external_python(task_id="etl_prod_options", python=PATH_TO_PYTHON)
def etl_prod_options(directory, Bucket):
    import travel_etl.det.offers as offers
    import travel_etl.prod.options as options

    det_offers = offers.DetOffers(directory)
    prod_options = options.ProdOptions(directory, Bucket)
    cfg = {
        "source": det_offers,
    }
    prod_options.load(**cfg)


with DAG(
    dag_id="etl_create_det_offers",
    catchup=False,
    schedule_interval=SCHEDULE,
    start_date=datetime.datetime(2023, 3, 1),
) as dag:
    task_start = BashOperator(task_id="start_task", bash_command="date", dag=dag)

    # load_travelata_task = etl_det_travelata(HOURS, DIRECTORY)
    load_teztour_task = etl_det_teztour(HOURS, DIRECTORY)
    load_pivot_task = etl_det_pivot(HOURS, DIRECTORY)
    load_offers_task = etl_det_offers(HOURS, DIRECTORY, DAYS_OFFER)
    load_offers_first_time_task = etl_det_offers_first_time(DIRECTORY, DAYS_OFFER_FIRST_TIME)
    load_prod_offers_task = etl_prod_offers(DIRECTORY, DAYS_OFFER)
    load_options_task = etl_prod_options(DIRECTORY, BUCKET)

    comb = task_start >> [load_teztour_task] >> load_pivot_task >> load_offers_task
    comb >> load_options_task
    comb >> load_offers_first_time_task >> load_prod_offers_task


@task.external_python(task_id="etl_stat_global_stats", python=PATH_TO_PYTHON)
def etl_stat_global_stats(directory, days):
    import travel_etl.stat.global_stat as global_stat

    stat_global_stat = global_stat.StatGlobalStat(directory)

    cfg = {"source": "parser/parsing_stat_raws", "days": days}
    stat_global_stat.load_table(**cfg)


with DAG(
    dag_id="etl_create_stat_global_stat",
    catchup=False,
    schedule_interval=SCHEDULE_STAT,
    start_date=datetime.datetime(2023, 3, 1),
) as dag:
    task_start = BashOperator(task_id="start_task", bash_command="date", dag=dag)
    load_global_stats_task = etl_stat_global_stats(DIRECTORY, DAYS_STAT)
    task_start >> load_global_stats_task
