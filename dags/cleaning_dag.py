from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from include.movie.data_cleaning import clean_movie_details
from include.movie.cleaning2  import clean_streaming_performance
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

with DAG(
    'movie_details_cleaning_dag',
    default_args=default_args,
    description='A DAG for cleaning the movie details dataset',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False
) as dag:

    clean_data_task = PythonOperator(
        task_id='clean_movie_details_task',
        python_callable=clean_movie_details,
        op_kwargs={
            'bucket_name': 'store',
            'raw_path': 'raw/movie_details.csv',
            'clean_path': 'clean/movie_details_clean.csv',
            'conn_id': 'minio'  # Airflow connection ID for MinIO
        }
    )

    clean_streaming_data=PythonOperator(
        task_id="clean_streaming_details",
        python_callable=clean_streaming_performance,
        op_kwargs={
            'bucket_name': 'store',
            'raw_path': 'raw/streaming_performance.csv',
            'clean_path': 'clean/streaming_performance_clean.csv',
            'conn_id': 'minio'  # Airflow connection ID for MinIO
        }
    )

    # clean_data_task >> 
    clean_streaming_data
