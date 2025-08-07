from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta

with DAG(
    dag_id='dummy_check_dag_samyuktha', # A new, unique DAG ID
    start_date=datetime(2023, 1, 1), # A static start date in the past
    schedule_interval=None, # Set to None for manual trigger
    catchup=False,
) as dag:
    DummyOperator(task_id="start")
