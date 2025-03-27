from airflow import DAG
from astro import sql as aql
from astro.files import File
from astro.table import Table, Metadata
from airflow.hooks.base import BaseHook
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

# List of tables to load
tables_to_load = [
    {"file_path": "s3://store/curated/fact_table.csv", "table_name": "fact_table"},
    {"file_path": "s3://store/curated/dim_actor.csv", "table_name": "dim_actor"},
    {"file_path": "s3://store/curated/dim_platform.csv", "table_name": "dim_platform"},
    {"file_path": "s3://store/curated/dim_genre.csv", "table_name": "dim_genre"},
    {"file_path": "s3://store/curated/dim_region.csv", "table_name": "dim_region"},
    {"file_path": "s3://store/curated/dim_producer.csv", "table_name": "dim_producer"}
]

# Define DAG
with DAG(
    'dynamic_load_to_postgres_dag',
    default_args=default_args,
    description='A DAG to dynamically load fact and dimension tables into PostgreSQL',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False
) as dag:

    # Function to load tables dynamically
    def load_table(file_path, table_name):
        return aql.load_file(
            task_id=f"load_{table_name}",
            input_file=File(
                path=file_path,
                conn_id="minio"
            ),
            output_table=Table(
                name=table_name,
                conn_id="postgres",
                metadata=Metadata(
                    schema='public'
                )
            ),
            load_options={
                "aws_access_key_id": BaseHook.get_connection("minio").login,
                "aws_secret_access_key": BaseHook.get_connection("minio").password,
                "end_point_url": BaseHook.get_connection("minio").host
            }
        )

    # Loop through tables and dynamically create tasks
    load_tasks = []
    for table in tables_to_load:
        load_task = load_table(table["file_path"], table["table_name"])
        load_tasks.append(load_task)

    # Define task dependencies
    for i in range(1, len(load_tasks)):
        load_tasks[i - 1] >> load_tasks[i]
