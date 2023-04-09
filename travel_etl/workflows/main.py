import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

import os

import travel_etl.det.travelata as travelata
import travel_etl.det.teztour as teztour
import travel_etl.det.pivot as pivot
import travel_etl.prod.offers as offers


os.chdir(os.environ["AIRFLOW_HOME"])

DIRECTORY = "parser"
HOURS = str(12)

with DAG(
    dag_id="etl_create_det_offers",
    catchup=False,
    schedule_interval="0 * * * *",
    start_date=datetime.datetime(2023, 3, 1),
) as dag:
    det_travelata = travelata.DetTravelata(DIRECTORY)
    det_teztour = teztour.DetTeztour(DIRECTORY)
    det_pivot = pivot.DetPivot(DIRECTORY)
    prod_offers = offers.ProdOffers(DIRECTORY)

    task_start = BashOperator(
        task_id='start_task',
        bash_command='date',
        dag=dag
    )

    load_travelata_task = PythonOperator(
        task_id="etl_det_travelata",
        python_callable=det_travelata.load_table,
        dag=dag,
        op_kwargs={
            "source": "parser/raw/travelata",
            "hours": HOURS,
        },
    )

    load_teztour_task = PythonOperator(
        task_id="etl_det_teztour",
        python_callable=det_teztour.load_table,
        dag=dag,
        op_kwargs={
            "source": "parser/raw/teztour",
            "hours": HOURS,
        },
    )

    load_pivot_task = PythonOperator(
        task_id="etl_det_pivot",
        python_callable=det_pivot.load_table,
        dag=dag,
        op_kwargs={
            "source_teztour": det_teztour,
            "source_travelata": det_travelata,
            "hours": HOURS,
        },
    )

    load_offers_task = PythonOperator(
        task_id="etl_prod_offers",
        python_callable=prod_offers.load_table,
        op_kwargs={
            "source": det_pivot,
            "hours": HOURS,
            "days_offer": "4",
        },
        dag=dag
    )

    comb = task_start >> [load_travelata_task, load_teztour_task] >> load_pivot_task 
    
    comb >> load_offers_task
