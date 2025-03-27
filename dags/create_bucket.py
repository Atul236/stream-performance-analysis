from airflow.decorators import dag
from datetime import datetime
from airflow.operators.python import PythonOperator
from include.movie.task1 import create_bucket
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table,Metadata
from airflow.hooks.base import BaseHook
from minio import Minio


@dag(
    start_date=datetime(2025,3,20),
    schedule=None,
    catchup=False,
    tags=['example'],
)
def create_bucket_dag():

    load_to_dw=aql.load_file(
        task_id="load_to_dw",
        input_file=File(
            path=f"s3://store/raw/store_transactions.csv",
            conn_id="minio"
        ),
        output_table=Table(
            name="store",
            conn_id="postgres",
            metadata=Metadata(
                schema='public'
            )
        ),
        load_options={
            "aws_access_key_id":BaseHook.get_connection("minio").login,
            "aws_secret_access_key":BaseHook.get_connection("minio").password,
            "end_point_url":BaseHook.get_connection('minio').host
        }
    )
    load_to_dw

create_bucket_dag()