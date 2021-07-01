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
default_end_time = '2999-12-31 23:59:59'



transform_dim_orders_sql = "/include/transform/transform_dim_orders.sql"

transform_fact_orders_created_sql = "/include/transform/transform_fact_orders_created.sql"

transform_dim_products_sql = "/include/transform/transform_dim_products.sql"

with DAG(
    dag_id ="process_orders",
    start_date=datetime(2020,1,1),
    schedule_interval="@once",
    default_args=default_args,
    catchup=False,
) as dag:
    # products
    check_stg_products_csv_readiness = BashSensor(
        task_id="check_stg_product_csv_readiness",
        bash_command="""
            ls /data/raw/products_{{ ds }}.csv
        """,
    )

    normalize_products_csv= PythonOperator(
        task_id='normalize_products_csv',
        python_callable=normalize_csv,
        op_kwargs={
            'source': "/data/raw/products_{{ ds }}.csv",
            'target': "/data/stg/products_{{ ds }}.csv"
        },

    )

    create_stg_products_table = PostgresOperator(
        task_id="create_stg_products_table",
        postgres_conn_id=connection_id,
        sql="include/initial/stage_table/create_stg_products.sql",
    )

    load_products_to_stg_products_table = PythonOperator(
        task_id='load_products_to_stg_products_table',
        python_callable=load_csv_to_postgres,
        op_kwargs={
            'csv_filepath': "/data/stg/products_{{ ds }}.csv",
            'table_name': 'stg_products'
        },
    )

    check_stg_products_csv_readiness >> normalize_products_csv >> create_stg_products_table >> load_products_to_stg_products_table

    create_dim_products_table = PostgresOperator(
        task_id="create_dim_products_table",
        postgres_conn_id=connection_id,
        sql="include/initial/dim_table/create_dim_products.sql",
    )

    transform_dim_products_table = PostgresOperator(
        task_id="transform_dim_products_table",
        postgres_conn_id=connection_id,
        sql=transform_dim_products_sql
    )

    load_products_to_stg_products_table >> create_dim_products_table >> transform_dim_products_table

    # orders
    check_stg_orders_csv_readiness = BashSensor(
        task_id="check_stg_orders_csv_readiness",
        bash_command="""
            ls /data/raw/orders_{{ ds }}.csv
        """,
    )

    normalize_orders_csv = PythonOperator(
        task_id='normalize_orders_csv',
        python_callable=normalize_csv,
        op_kwargs={
            'source': "/data/raw/orders_{{ ds }}.csv",
            'target': "/data/stg/orders_{{ ds }}.csv",
        },
    )
    
    create_stg_orders_table = PostgresOperator(
        task_id="create_stg_orders_table",
        postgres_conn_id=connection_id,
        sql="/include/initial/stage_table/create_stg_orders.sql",
    )
    

    load_orders_to_stg_orders_table = PythonOperator(
        task_id="load_orders_to_stg_orders_table",
        python_callable=load_csv_to_postgres,
        op_kwargs={
            'csv_filepath': "/data/stg/orders_{{ ds }}.csv",
            'table_name': 'stg_orders'
        },
    )

    check_stg_orders_csv_readiness >> normalize_orders_csv >> create_stg_orders_table >> load_orders_to_stg_orders_table

    create_dim_orders_table = PostgresOperator(
        task_id="create_dim_orders_table",
        postgres_conn_id=connection_id,
        sql="/include/initial/dim_table/create_dim_orders.sql",
    )

    create_fact_orders_created_table = PostgresOperator(
        task_id="create_fact_orders_created_table",
        postgres_conn_id=connection_id,
        sql="/include/initial/fact_table/create_fact_ordered.sql"
    )
    
    load_orders_to_stg_orders_table >> [create_dim_orders_table, create_fact_orders_created_table]

    transform_dim_orders_table = PostgresOperator(
        task_id="transform_dim_orders_table",
        postgres_conn_id=connection_id,
        sql=transform_dim_orders_sql,
    )

    create_dim_orders_table >> transform_dim_orders_table

    transform_fact_orders_created_table = PostgresOperator(
        task_id="transform_fact_orders_created_table",
        postgres_conn_id=connection_id,
        sql=transform_fact_orders_created_sql,
    )

    create_fact_orders_created_table >> transform_fact_orders_created_table

