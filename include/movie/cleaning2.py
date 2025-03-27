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






# Set up logging
logging.basicConfig(level=logging.INFO)

def clean_streaming_performance(bucket_name, raw_path, clean_path, conn_id='minio'):
    """
    Cleans the raw streaming performance dataset and uploads the cleaned file to MinIO.

    Args:
        bucket_name (str): Name of the MinIO bucket.
        raw_path (str): Path to the raw file in MinIO.
        clean_path (str): Path to save the cleaned file in MinIO.
        conn_id (str): Airflow connection ID for MinIO (default: 'minio').

    Returns:
        None
    """
    try:
        # Initialize MinIO client and ensure the bucket exists
        client = create_minio_client(conn_id)
        ensure_bucket(client, bucket_name)

        # Read raw data from MinIO
        logging.info(f"Downloading raw file from {bucket_name}/{raw_path}...")
        raw_file_obj = client.get_object(bucket_name, raw_path)
        streaming_df = pd.read_csv(io.BytesIO(raw_file_obj.read()))

        # Cleaning data
        logging.info("Cleaning streaming performance data...")

        # Convert 'streaming_revenue_usd' to numeric and fill missing values with the mean
        streaming_df['streaming_revenue_usd'] = streaming_df['streaming_revenue_usd'].replace(
            'Two Hundred Thousand', 200000).astype(float)
        mean_revenue = streaming_df['streaming_revenue_usd'].mean()
        streaming_df['streaming_revenue_usd'].fillna(mean_revenue, inplace=True)

        # Replace missing watch hours and monthly views with 0
        streaming_df['watch_hours'].fillna(0, inplace=True)
        streaming_df['monthly_views'].fillna(0, inplace=True)

        # Replace slashes in dates and standardize to YYYY-MM-DD
        streaming_df['streaming_date'] = streaming_df['streaming_date'].str.replace('/', '-', regex=False)
        streaming_df['streaming_date'] = pd.to_datetime(
            streaming_df['streaming_date'], errors='coerce', format='%Y-%m-%d'
        )

        # Drop rows with invalid or missing dates
        streaming_df = streaming_df.dropna(subset=['streaming_date'])

        # Remove special characters in 'feedback'
        streaming_df['feedback'] = streaming_df['feedback'].str.replace(r'[^\w\s]', '', regex=True)

        # Remove duplicate rows
        streaming_df.drop_duplicates(inplace=True)

        # Validate movie_id (ensure it's in range 1-100)
        valid_movie_ids = list(range(1, 101))  # Assuming movie_ids are 1 to 100 from movie_details.csv
        streaming_df = streaming_df[streaming_df['movie_id'].isin(valid_movie_ids)]

        # Save cleaned data back to MinIO
        logging.info(f"Uploading cleaned file to {bucket_name}/{clean_path}...")
        csv_buffer = io.StringIO()
        streaming_df.to_csv(csv_buffer, index=False)
        client.put_object(
            bucket_name,
            clean_path,
            data=io.BytesIO(csv_buffer.getvalue().encode('utf-8')),
            length=len(csv_buffer.getvalue().encode('utf-8')),
            content_type='text/csv'
        )

        logging.info("Streaming performance cleaning process completed successfully!")

    except Exception as e:
        logging.error(f"Error during streaming performance cleaning: {e}")
        raise
