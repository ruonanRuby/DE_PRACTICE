from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

connection_id = 'dwh'

with DAG(
    dag_id ="create_dim_dates",
    start_date=datetime(2020,2,2),
    schedule_interval="@once",
    default_args=default_args,
    catchup=False,
) as dag:
    create_dim_dates_table = PostgresOperator(
        task_id="create_dim_dates_table",
        postgres_conn_id=connection_id,
        sql='/include/initial/dim_table/create_dim_dates.sql'
    )

    create_dim_dates_table