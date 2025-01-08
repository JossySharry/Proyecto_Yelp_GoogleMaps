import os
import pandas as pd
from google.cloud import storage, bigquery

# Configuración del cliente de GCS y BigQuery
storage_client = storage.Client()
bq_client = bigquery.Client()

# Leer el nombre del bucket desde las variables de entorno
BUCKET_NAME = os.getenv('BUCKET_NAME', 'saborlatino')

# Función para leer un archivo Parquet desde GCS
def read_from_gcs(bucket_name, folder_name, file_name):
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(f'{folder_name}/{file_name}')
    local_path = f"/tmp/{file_name}"
    blob.download_to_filename(local_path)
    return pd.read_parquet(local_path)

# Función para cargar DataFrame en BigQuery
def load_to_bigquery(df, table_id, schema):
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition="WRITE_APPEND",  
    )
    job = bq_client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()  # Esperar a que el trabajo se complete

# Esquemas para las tablas
restaurants_schema = [
    bigquery.SchemaField("restaurant_id", "STRING"),
    bigquery.SchemaField("restaurant_name", "STRING"),
    bigquery.SchemaField("address", "STRING"),
    bigquery.SchemaField("category_id", "INTEGER"),  # Asegúrate de que sea INTEGER en el esquema
]

reviews_schema = [
    bigquery.SchemaField("review_id", "STRING"),
    bigquery.SchemaField("restaurant_id", "STRING"),
    bigquery.SchemaField("user_id", "STRING"),
    bigquery.SchemaField("rating", "INTEGER"),
    bigquery.SchemaField("text", "STRING")
]

users_schema = [
    bigquery.SchemaField("user_id", "STRING"),
    bigquery.SchemaField("user_name", "STRING")
]

# Función principal de la GCF
def process_data_api(request):
    # Carpetas y archivos procesados
    input_folder = 'processed'
    try:
        # Leer los datasets desde GCS
        df_restaurants = read_from_gcs(BUCKET_NAME, input_folder, 'restaurants_api_processed.parquet')
        df_reviews = read_from_gcs(BUCKET_NAME, input_folder, 'reviews_api_processed.parquet')
        df_users = read_from_gcs(BUCKET_NAME, input_folder, 'users_api_processed.parquet')

        # Convertir 'category_id' a entero
        df_restaurants['category_id'] = pd.to_numeric(df_restaurants['category_id'], errors='coerce', downcast='integer')
        df_restaurants = df_restaurants.dropna(subset=['category_id'])  # Eliminar NaN tras la conversión

        # Validaciones de duplicados
        query_restaurants = f"""
            SELECT restaurant_id FROM `expansionsaborlatino.Saborlatino.restaurants`
        """
        existing_restaurants = bq_client.query(query_restaurants).to_dataframe()
        df_restaurants = df_restaurants[~df_restaurants['restaurant_id'].isin(existing_restaurants['restaurant_id'])]

        query_reviews = f"""
            SELECT text FROM `expansionsaborlatino.Saborlatino.reviews`
        """
        existing_reviews = bq_client.query(query_reviews).to_dataframe()
        df_reviews = df_reviews[~df_reviews['text'].isin(existing_reviews['text'])]

        query_users = f"""
            SELECT user_id FROM `expansionsaborlatino.Saborlatino.users`
        """
        existing_users = bq_client.query(query_users).to_dataframe()
        df_users = df_users[~df_users['user_id'].isin(existing_users['user_id'])]

        # Cargar los datos validados a BigQuery
        load_to_bigquery(df_restaurants, 'expansionsaborlatino.Saborlatino.restaurants', restaurants_schema)
        load_to_bigquery(df_reviews, 'expansionsaborlatino.Saborlatino.reviews', reviews_schema)
        load_to_bigquery(df_users, 'expansionsaborlatino.Saborlatino.users', users_schema)

        return 'Data validated and loaded to BigQuery successfully.'

    except Exception as e:
        print(f'Error processing data: {e}')
        return f'Error processing data: {e}'
