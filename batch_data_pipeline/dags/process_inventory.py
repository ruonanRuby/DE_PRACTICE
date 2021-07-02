from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

from airflow.operators.python import PythonOperator
from airflow.sensors.bash import BashSensor

from include.utils.csv_utils import normalize_csv, load_csv_to_postgres

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
        dag_id="process_inventory",
        start_date=datetime(2020,1,1),
        schedule_interval="@once",
        default_args=default_args,
        catchup=False,
) as dag:
    check_inventory_csv_readiness = BashSensor(
        task_id="check_inventory_csv_readiness",
        bash_command="""
            ls /data/raw/inventory_{{ ds }}.csv
        """,
    )

    normalize_inventory_csv = PythonOperator(
        task_id='normalize_inventory_csv',
        python_callable=normalize_csv,
        op_kwargs={
            'source': "/data/raw/inventory_{{ ds }}.csv",
            'target': "/data/stg/inventory_{{ ds }}.csv"
        },
    )

    create_fact_inventory_snapshot_table = PostgresOperator(
        task_id="create_fact_inventory_snapshot_table",
        postgres_conn_id=connection_id,
        sql="include/initial/fact_table/create_fact_inventory_snapshot.sql",
    )

    load_inventory_to_inventory_snapshot_table = PythonOperator(
        task_id="load_inventory_to_inventory_snapshot_table",
        python_callable=load_csv_to_postgres,
        op_kwargs={
            'csv_filepath': "../../data/stg/inventory_{{ ds }}.csv",
            'table_name': 'fact_inventory_snapshot'
        },
    )

    check_inventory_csv_readiness >> normalize_inventory_csv >> create_fact_inventory_snapshot_table >> load_inventory_to_inventory_snapshot_table



