from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

with DAG(
    dag_id='simple_hello_world_dag', # A new, unique DAG ID
    start_date=datetime(2023, 1, 1), # A static start date in the past
    schedule_interval=None, # Set to None for manual trigger
    catchup=False,
    tags=['test', 'hello_world'],
) as dag:
    # Define a simple task that just prints a message
    hello_task = BashOperator(
        task_id='say_hello',
        bash_command='echo "Hello from Samyuktha\'s simple DAG!"',
    )
