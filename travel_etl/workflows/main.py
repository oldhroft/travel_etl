import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

import os

import travel_etl.det.travelata as travelata
import travel_etl.det.teztour as teztour

os.chdir(os.environ["AIRFLOW_HOME"])

with DAG(
    dag_id="main_etl",
    catchup=False,
    schedule_interval=None,
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

    load_travelata_task >> load_teztour_task