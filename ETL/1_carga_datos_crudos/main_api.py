import pandas as pd
import requests
import googlemaps
from google.cloud import storage
import os  # Para leer las variables de entorno

# Configuración del cliente de GCS
storage_client = storage.Client()

# Leer el nombre del bucket desde las variables de entorno
BUCKET_NAME = os.getenv('BUCKET_NAME', 'saborlatino')  

# Función para guardar un DataFrame como Parquet en GCS
def save_to_gcs(df, bucket_name, folder_name, file_name):
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(f'{folder_name}/{file_name}')
    local_path = f"/tmp/{file_name}"
    df.to_parquet(local_path, index=False)
    blob.upload_from_filename(local_path)

# Tu clave de API de Google Places
API_KEY = 'AIzaSyAryLAe3aiFS2xRbwDXcLUxw7wrfVZx7ZI'

# Función para la extracción de datos
def extract_data(request):

    output_folder = 'raw'
    gmaps = googlemaps.Client(key=API_KEY)

    # Configuración de ciudades con mayor población latina
    locations = {
        "Texas": "29.7604,-95.3698",  # Houston
        "Florida": "25.7617,-80.1918",  # Miami
        "California": "34.0522,-118.2437",  # Los Ángeles
        "Illinois": "41.8781,-87.6298",  # Chicago
        "New York": "40.7128,-74.0060",  # Nueva York
        "Arizona": "33.4484,-112.0740",  # Phoenix
    }
    radius = 1000  # En metros
    query = "Latin American Restaurant"
    type = "restaurant"

    try:
        # Procesar resultados
        def process_results(response):
            places = []
            for result in response.get('results', []):
                place = {
                    'place_id': result.get('place_id'),
                    'name': result.get('name'),
                    'address': result.get('formatted_address', result.get('vicinity')),
                    'category': 'Latin American restaurant',
                    'latitude': result['geometry']['location']['lat'],
                    'longitude': result['geometry']['location']['lng'],
                    'rating': result.get('rating'),
                    'user_ratings_total': result.get('user_ratings_total'),
                }
                places.append(place)
            return pd.DataFrame(places)

        # Extraer y concatenar datos de todas las locaciones
        all_places = []
        for state, location in locations.items():
            response = gmaps.places(query=query, location=location, radius=radius, type=type)
            df_places = process_results(response)
            df_places['state'] = state  # Agregar información del estado
            all_places.append(df_places)

        df_places_combined = pd.concat(all_places, ignore_index=True)

        # Obtener detalles y reseñas
        def get_place_reviews(place_id):
            details = gmaps.place(place_id=place_id)
            reviews = details.get('result', {}).get('reviews', [])
            formatted_reviews = []
            for review in reviews:
                formatted_reviews.append({
                    'place_id': place_id,
                    'user_id': review.get('author_url', '').split('/')[5],
                    'author_name': review.get('author_name'),
                    'rating': review.get('rating'),
                    'text': review.get('text'),
                    'time': review.get('relative_time_description'),
                })
            return formatted_reviews

        all_reviews = []
        for place_id in df_places_combined['place_id']:
            reviews = get_place_reviews(place_id)
            all_reviews.extend(reviews)

        df_reviews = pd.DataFrame(all_reviews)

        # Guardar los resultados en GCS
        save_to_gcs(df_places_combined, BUCKET_NAME, output_folder, 'places_combined.parquet')
        save_to_gcs(df_reviews, BUCKET_NAME, output_folder, 'reviews_api.parquet')
        return 'Data processed and saved successfully.'

    except Exception as e:
        print(f'Error processing data: {e}')
        return f'Error processing data: {e}'