from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from include.movie.dimension_modeling import generate_star_schema
from datetime import datetime


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

with DAG(
    'dimension_modeling_dag',
    default_args=default_args,
    description='A DAG for generating star schema (fact and dimension tables)',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False
) as dag:

    dimension_modeling_task = PythonOperator(
        task_id='generate_star_schema_task',
        python_callable=generate_star_schema,
        op_kwargs={
            'bucket_name': 'store',
            'clean_movie_path': 'clean/movie_details_clean.csv',
            'clean_streaming_path': 'clean/streaming_performance_clean.csv',
            'fact_path': 'curated/fact_table.csv',
            'dim_folder': 'curated',
            'conn_id': 'minio'
        }
    )

    dimension_modeling_task
