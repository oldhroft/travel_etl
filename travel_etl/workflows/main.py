import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

import os

import travel_etl.det.travelata as travelata
import travel_etl.det.teztour as teztour

import travel_etl.det.pivot as pivot
import travel_etl.prod.offers as offers


os.chdir(os.environ["AIRFLOW_HOME"])

with DAG(
    dag_id="main_etl",
    catchup=False,
    schedule_interval="0 * * * *",
    start_date=datetime.datetime(1970, 1, 1),
) as dag:
    
    load_travelata_task  = PythonOperator(
        task_id="etl_det_travelata", python_callable=travelata.load,
        dag=dag, 
    )

    load_teztour_task = PythonOperator(
        task_id="etl_det_teztour", python_callable=teztour.load,
        dag=dag, 
    )

    load_pivot_task = PythonOperator(
        task_id="etl_det_pivot", python_callable=pivot.load,
        dag=dag, 
    )

    load_offers_task = PythonOperator(
        task_id="etl_prod_offers", python_callable=offers.load,
        dag=dag, 
    )

    load_travelata_task >> load_teztour_task >> load_pivot_task >> load_offers_task