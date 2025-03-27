from airflow.hooks.base import BaseHook
from minio import Minio
from minio.error import S3Error
import pandas as pd
import io
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)

def create_minio_client(conn_id='minio'):
    """
    Creates a MinIO client using Airflow's BaseHook connection.

    Args:
        conn_id (str): Airflow connection ID for MinIO (default: 'minio').

    Returns:
        Minio: Configured MinIO client.
    """
    # Retrieve MinIO connection from Airflow's Connection UI
    minio_conn = BaseHook.get_connection(conn_id)
    client = Minio(
        endpoint=minio_conn.extra_dejson['endpoint_url'].split('//')[1],
        access_key=minio_conn.login,
        secret_key=minio_conn.password,
        secure=False  # Adjust based on your MinIO setup
    )
    return client

def ensure_bucket(client, bucket_name):
    """
    Ensures the specified bucket exists in MinIO. Creates it if it does not exist.

    Args:
        client (Minio): MinIO client.
        bucket_name (str): Name of the bucket to ensure.

    Returns:
        None
    """
    try:
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            logging.info(f"Bucket '{bucket_name}' created successfully.")
        else:
            logging.info(f"Bucket '{bucket_name}' already exists.")
    except S3Error as e:
        logging.error(f"Error ensuring bucket '{bucket_name}': {e}")
        raise

def clean_movie_details(bucket_name, raw_path, clean_path, conn_id='minio'):
    """
    Cleans the raw movie details dataset and uploads the cleaned file to MinIO.

    Args:
        bucket_name (str): Name of the MinIO bucket.
        raw_path (str): Path to the raw file in MinIO.
        clean_path (str): Path to save the cleaned file in MinIO.
        conn_id (str): Airflow connection ID for MinIO (default: 'minio').

    Returns:
        None
    """
    try:
        # Step 1: Initialize MinIO client and ensure the bucket exists
        client = create_minio_client(conn_id)
        ensure_bucket(client, bucket_name)

        # Step 2: Read raw data from MinIO
        logging.info(f"Downloading raw file from {bucket_name}/{raw_path}...")
        raw_file_obj = client.get_object(bucket_name, raw_path)
        movie_details_df = pd.read_csv(io.BytesIO(raw_file_obj.read()))

        # Step 3: Apply Data Cleaning
        logging.info("Cleaning data...")
        # Drop rows where 'movie_id' or 'movie' is missing
        movie_details_df.dropna(subset=['movie_id', 'movie'], inplace=True)

        # Fill missing values with placeholders
        movie_details_df.fillna({
            'genre': 'Unknown',
            'region': 'Unknown',
            'release_date': '0000-00-00',
            'lead_actor': 'TBD',
            'producer': 'TBD'
        }, inplace=True)

        # Remove special characters in 'comments'
        movie_details_df['comments'] = movie_details_df['comments'].str.replace(r'[^\w\s]', '', regex=True)

        # Step 4: Save cleaned data back to MinIO
        logging.info(f"Uploading cleaned file to {bucket_name}/{clean_path}...")
        csv_buffer = io.StringIO()
        movie_details_df.to_csv(csv_buffer, index=False)
        client.put_object(
            bucket_name,
            clean_path,
            data=io.BytesIO(csv_buffer.getvalue().encode('utf-8')),
            length=len(csv_buffer.getvalue().encode('utf-8')),
            content_type='text/csv'
        )

        logging.info("Data cleaning and upload process completed successfully!")

    except Exception as e:
        logging.error(f"Error during data cleaning process: {e}")
        raise
