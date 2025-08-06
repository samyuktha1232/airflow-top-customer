from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.python.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta # Added for more flexible date/time handling
import pandas as pd
import os

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def export_to_csv_callable():
    hook = PostgresHook(postgres_conn_id='neon_postgres')
    engine = hook.get_sqlalchemy_engine()
    df = pd.read_sql("SELECT * FROM temp_top_customers", con=engine)

    
    output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'include')
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, 'top_10_customers.csv')
    df.to_csv(output_path, index=False)
    print(f"Data exported to {output_path}") # Added a print statement for logging

with DAG(
    dag_id='export_top_customers_dag', # Changed DAG ID for clarity
    default_args=default_args,
    description='A DAG to export top customers to a CSV file',
    start_date=datetime(2023, 1, 1), # Set a specific start date
    schedule_interval=None, # Set to None for manual trigger, or use '0 0 * * *' for daily
    catchup=False,
    tags=['customer_export', 'postgres'],
) as dag:
    # Step 1: Define the task to create a temporary table
    create_temp_table = SQLExecuteQueryOperator(
        task_id='create_top_customers_temp',
        conn_id='neon_postgres',  # Your connection ID from Airflow
        sql="""
        DROP TABLE IF EXISTS temp_top_customers;
        CREATE TEMP TABLE temp_top_customers AS
        SELECT c.Customer_Id, c.First_Name, c.Last_Name, SUM(i.Total) AS TotalAmount
        FROM Customer c
        JOIN Invoice i ON c.Customer_Id = i.Customer_Id
        GROUP BY c.Customer_Id
        ORDER BY TotalAmount DESC
        LIMIT 10;
        """
    )

    export_task = PythonOperator(
        task_id='export_customers_to_csv', # Changed task_id for clarity
        python_callable=export_to_csv_callable, # Referencing the function defined above
    )

    # Define task dependencies: create_temp_table must run before export_task
    create_temp_table >> export_task
