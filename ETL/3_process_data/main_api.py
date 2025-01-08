import os
import pandas as pd
from google.cloud import storage

# Configuración del cliente de GCS
storage_client = storage.Client()

# Leer el nombre del bucket desde las variables de entorno
BUCKET_NAME = os.getenv('BUCKET_NAME', 'saborlatino')  

# Función para leer un archivo Parquet desde GCS
def read_from_gcs(bucket_name, folder_name, file_name):
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(f'{folder_name}/{file_name}')
    local_path = f"/tmp/{file_name}"
    blob.download_to_filename(local_path)
    return pd.read_parquet(local_path)

# Función para guardar un DataFrame como Parquet en GCS
def save_to_gcs(df, bucket_name, folder_name, file_name):
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(f'{folder_name}/{file_name}')
    local_path = f"/tmp/{file_name}"
    df.to_parquet(local_path, index=False)
    blob.upload_from_filename(local_path)

# Función principal de transformación
def process_data(request):
    # Parámetros configurables
    input_folder = 'raw'       # Carpeta de entrada
    output_folder = 'processed'  # Carpeta de salida

    try:
        # Leer los datasets desde GCS
        df_places_combined= read_from_gcs(BUCKET_NAME, input_folder, 'places_combined.parquet')
        df_reviews = read_from_gcs(BUCKET_NAME, input_folder, 'reviews_api.parquet')
        categories = read_from_gcs(BUCKET_NAME, output_folder, 'categories_processed.parquet')
        census = read_from_gcs(BUCKET_NAME, input_folder, 'census.parquet')
        states = read_from_gcs(BUCKET_NAME, input_folder, 'states.parquet')

        # Aplicar las transformaciones
        df_places_combined = df_places_combined.rename(columns={'place_id': 'restaurant_id', 'name': 'restaurant_name'})
        df_places_combined.drop(columns=['rating', 'user_ratings_total'], inplace=True)
        df_places_combined = df_places_combined.rename(columns={'state': 'abbreviation'})
        df_places_combined['abbreviation'] = df_places_combined['abbreviation'].replace({
            'Texas': 'TX', 'Florida': 'FL', 'California': 'CA',
            'Illinois': 'IL', 'New York': 'NY', 'Arizona': 'AZ'
        })
        nuevo_orden = ['restaurant_id', 'restaurant_name', 'address', 'latitude', 'longitude', 'category', 'abbreviation']
        df_places_combined = df_places_combined.reindex(columns=nuevo_orden)
        df_places_combined = df_places_combined.rename(columns={'category': 'category_name'})
        df_reviews = df_reviews.rename(columns={'place_id': 'restaurant_id'})
        df_reviews.drop(columns=['time'], inplace=True)
        df_reviews['review_id'] = df_reviews['restaurant_id'].str[:10] + '_' + df_reviews['user_id'].str[:10]
        df_users = df_reviews[['user_id', 'author_name']].drop_duplicates(subset='user_id')
        df_reviews.drop(columns=['author_name'], inplace=True)
        df_reviews = df_reviews.reindex(columns=['review_id', 'restaurant_id', 'user_id', 'rating', 'text'])
        df_users = df_users.rename(columns={'author_name': 'user_name'})
        states = pd.merge(census, states, on='state_name', how='inner')
        states = states.rename(columns={'total_population_y': 'total_population', 'hispanic_population_y': 'hispanic_population', 'avg_income_y': 'avg_income'})
        states = states.reindex(columns=['state_id', 'state_name', 'abbreviation', 'total_population', 'hispanic_population', 'avg_income'])
        states['avg_income'] = states['avg_income'].round(2)
        states['hispanic_population'] = states['hispanic_population'].round().astype(int)
        df_places_combined = pd.merge(df_places_combined, categories, on='category_name', how='inner').drop(columns=['category_name'])
        df_places_combined = pd.merge(df_places_combined, states, on='abbreviation', how='inner').drop(columns=['abbreviation', 'state_name', 'total_population', 'hispanic_population', 'avg_income'])

        # Guardar los resultados en GCS
        save_to_gcs(df_places_combined, BUCKET_NAME, output_folder, 'restaurants_api_processed.parquet')
        save_to_gcs(df_reviews, BUCKET_NAME, output_folder, 'reviews_api_processed.parquet')
        save_to_gcs(df_users, BUCKET_NAME, output_folder, 'users_api_processed.parquet')


        return 'Data processed and saved successfully.'

    except Exception as e:
            print(f'Error processing data: {e}')
            return f'Error processing data: {e}'