from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
import os

default_args = {
    'owner': 'airflow',
}

with DAG(
    dag_id='export_top_customers_operator',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=['example'],
) as dag:

    # Step 1: Create a temporary table with top 10 customers
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

    # Step 2: Export results to CSV using pandas
    def export_to_csv():
        hook = PostgresHook(postgres_conn_id='neon_postgres')
        engine = hook.get_sqlalchemy_engine()

        df = pd.read_sql("SELECT * FROM temp_top_customers", con=engine)

        output_dir = os.path.join(os.path.dirname(__file__), '../include')
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, 'top_10_customers.csv')
        df.to_csv(output_path, index=False)

    export_task = PythonOperator(
        task_id='export_csv',
        python_callable=export_to_csv
    )

    create_temp_table >> export_task
