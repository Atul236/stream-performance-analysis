import pandas as pd
import io  # For in-memory CSV processing
import logging  # For logging messages
from minio import Minio  # MinIO client
from airflow.hooks.base import BaseHook
from minio.error import S3Error


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






def generate_star_schema(bucket_name, clean_movie_path, clean_streaming_path, fact_path, dim_folder, conn_id='minio'):
    """
    Creates a star schema (fact and dimension tables) and uploads them to MinIO.

    Args:
        bucket_name (str): Name of the MinIO bucket.
        clean_movie_path (str): Path to the cleaned movie details file in MinIO.
        clean_streaming_path (str): Path to the cleaned streaming performance file in MinIO.
        fact_path (str): Path to save the fact table in MinIO.
        dim_folder (str): Path to save dimension tables in MinIO.
        conn_id (str): Airflow connection ID for MinIO (default: 'minio').

    Returns:
        None
    """
    try:
        # Initialize MinIO client
        client = create_minio_client(conn_id)
        ensure_bucket(client, bucket_name)

        # Download and load cleaned movie details
        logging.info(f"Downloading cleaned movie details from {bucket_name}/{clean_movie_path}...")
        movie_obj = client.get_object(bucket_name, clean_movie_path)
        movie_details_df = pd.read_csv(io.BytesIO(movie_obj.read()))

        # Download and load cleaned streaming performance
        logging.info(f"Downloading cleaned streaming performance from {bucket_name}/{clean_streaming_path}...")
        streaming_obj = client.get_object(bucket_name, clean_streaming_path)
        streaming_df = pd.read_csv(io.BytesIO(streaming_obj.read()))

        # Create Fact Table
        logging.info("Generating Fact Table...")
        fact_table = streaming_df.groupby(['movie_id', 'platform'], as_index=False).agg({
            'streaming_revenue_usd': 'sum',
            'watch_hours': 'sum',
            'monthly_views': 'sum'
        })

        # Save Fact Table to MinIO in the curated folder
        fact_path = f"curated/fact_table.csv"
        logging.info(f"Uploading Fact Table to {bucket_name}/{fact_path}...")
        fact_csv_buffer = io.StringIO()
        fact_table.to_csv(fact_csv_buffer, index=False)
        client.put_object(
            bucket_name,
            fact_path,
            data=io.BytesIO(fact_csv_buffer.getvalue().encode('utf-8')),
            length=len(fact_csv_buffer.getvalue().encode('utf-8')),
            content_type='text/csv'
        )

        # Create Dimension Tables
        logging.info("Generating Dimension Tables...")
        
        # Actor Dimension
        actor_dim = movie_details_df[['lead_actor', 'movie_id']]
        actor_dim_path = f"{dim_folder}/dim_actor.csv"

        # Platform Dimension
        platform_dim = fact_table.groupby('platform', as_index=False).agg({
            'streaming_revenue_usd': 'sum',
            'watch_hours': 'sum'
        })
        platform_dim_path = f"{dim_folder}/dim_platform.csv"

        # Genre Dimension
        genre_dim = movie_details_df.merge(streaming_df, on='movie_id').groupby('genre', as_index=False).agg({
            'monthly_views': 'sum',
            'watch_hours': 'sum'
        })
        genre_dim_path = f"{dim_folder}/dim_genre.csv"

        # Region Dimension
        region_dim = movie_details_df.merge(streaming_df, on='movie_id').groupby('region', as_index=False).agg({
            'movie_id': 'count',
            'monthly_views': 'mean'
        }).rename(columns={'movie_id': 'total_movies', 'monthly_views': 'average_views'})
        region_dim_path = f"{dim_folder}/dim_region.csv"

        # Producer Dimension
        producer_dim = movie_details_df.groupby('producer', as_index=False).agg({
            'movie_id': 'count'
        }).merge(streaming_df, on='movie_id').groupby('producer', as_index=False).agg({
            'streaming_revenue_usd': 'sum'
        })
        producer_dim_path = f"{dim_folder}/dim_producer.csv"

        # Save Dimension Tables to MinIO
        for dim_table, dim_path in [
            (actor_dim, actor_dim_path), (platform_dim, platform_dim_path),
            (genre_dim, genre_dim_path), (region_dim, region_dim_path),
            (producer_dim, producer_dim_path)
        ]:
            logging.info(f"Uploading Dimension Table to {bucket_name}/{dim_path}...")
            dim_csv_buffer = io.StringIO()
            dim_table.to_csv(dim_csv_buffer, index=False)
            client.put_object(
                bucket_name,
                dim_path,
                data=io.BytesIO(dim_csv_buffer.getvalue().encode('utf-8')),
                length=len(dim_csv_buffer.getvalue().encode('utf-8')),
                content_type='text/csv'
            )

        logging.info("Fact and Dimension Tables created and uploaded successfully.")

    except Exception as e:
        logging.error(f"Error during star schema generation: {e}")
        raise
