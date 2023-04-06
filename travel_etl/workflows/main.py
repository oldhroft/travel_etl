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
    dag_id="etl_create_det_offers",
    catchup=False,
    schedule_interval="0 * * * *",
    start_date=datetime.datetime(2023, 3, 1),
) as dag:
    
    base_dir = "parser"
    det_travelata = travelata.DetTravelata(base_dir)
    det_teztour = teztour.DetTeztour(base_dir)
    det_pivot = pivot.DetPivot(base_dir)
    prod_offers = offers.ProdOffers(base_dir)

    load_travelata_task = PythonOperator(
        task_id="etl_det_travelata",
        python_callable=det_travelata.load_table,
        dag=dag,
        op_kwargs={
            "source": "parser/raw/travelata",
            "hours": "6",
        },
    )

    load_teztour_task = PythonOperator(
        task_id="etl_det_teztour",
        python_callable=det_teztour.load_table,
        dag=dag,
        op_kwargs={
            "source": "parser/raw/teztour",
            "hours": "6",
        },
    )

    load_pivot_task = PythonOperator(
        task_id="etl_det_pivot",
        python_callable=det_pivot.load_table,
        dag=dag,
        op_kwargs={
            "source_teztour": det_teztour,
            "source_travelata": det_travelata,
            "hours": "6",
        },
    )

    load_offers_task = PythonOperator(
        task_id="etl_prod_offers",
        python_callable=prod_offers.load_table,
        op_kwargs={
            "source": det_pivot,
            "hours": "6",
            "days_offer": "4",
        },
    )

    load_travelata_task >> load_teztour_task >> load_pivot_task >> load_offers_task
