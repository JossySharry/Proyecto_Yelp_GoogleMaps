import os  # Para leer las variables de entorno
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
        latin_restaurants_google = read_from_gcs(BUCKET_NAME, input_folder, 'latin_restaurants_google.parquet')
        latin_restaurants_yelp = read_from_gcs(BUCKET_NAME, input_folder, 'latin_restaurants_yelp.parquet')
        reviews_google = read_from_gcs(BUCKET_NAME, input_folder, 'reviews_google.parquet')
        reviews_yelp = read_from_gcs(BUCKET_NAME, input_folder, 'reviews_yelp.parquet')
        users_yelp = read_from_gcs(BUCKET_NAME, input_folder, 'users_yelp.parquet')
        census = read_from_gcs(BUCKET_NAME, input_folder, 'census.parquet')
        states = read_from_gcs(BUCKET_NAME, input_folder, 'states.parquet')

        # Transformaciones
        latin_restaurants_google = latin_restaurants_google.rename(columns={'gmap_id': 'restaurant_id', 'name': 'restaurant_name'})
        latin_restaurants_google.drop(columns=['description', 'avg_rating', 'num_of_reviews', 'price', 'hours', 
                                               'state', 'relative_results', 'url', 'MISC'], inplace=True)
        print('Restaurantes latinos de Google limpios en base al diagrama E-R')

        latin_restaurants_yelp = latin_restaurants_yelp.rename(columns={'business_id': 'restaurant_id', 
                                                                        'name': 'restaurant_name', 
                                                                        'restaurante_categoria': 'category'})
        latin_restaurants_yelp.drop(columns=['city', 'postal_code', 'stars', 'review_count', 'is_open', 
                                             'attributes', 'hours', 'is_food_related', 'categories'], inplace=True)
        print('Restaurantes latinos de Yelp limpios en base al diagrama E-R')

        reviews_google = reviews_google.rename(columns={'gmap_id': 'restaurant_id'})
        reviews_google.drop(columns=['time', 'pics', 'resp'], inplace=True)
        print('Reviews por estados limpias')

        # Normalización de categorías
        category_mapping = {
            'Burrito restaurant': 'Mexican restaurant',
            'Taco restaurant': 'Mexican restaurant',
            'Oaxacan restaurant': 'Mexican restaurant',
            'Nuevo Latino restaurant': 'South American restaurant',
            'Mexican torta restaurant': 'Mexican American restaurant',
            'Pan-Latin restaurant': 'South American restaurant',
            'Mexican American restaurant': 'Mexican restaurant'
        }
        latin_restaurants_google['category'] = latin_restaurants_google['category'].replace(category_mapping)
        print('Categorías de restaurantes latinos de Google normalizadas')

        ## Asignando orden a las columnas
        nuevo_orden = ['restaurant_id', 'restaurant_name', 'address', 'latitude', 'longitude', 'category', 'abbreviation']
        latin_restaurants_google = latin_restaurants_google.reindex(columns=nuevo_orden)
        latin_restaurants_yelp = latin_restaurants_yelp.reindex(columns=nuevo_orden)
        print('Orden de columnas asignado')

        # Concatenación de los restaurantes de Google y Yelp
        latin_restaurants = pd.concat([latin_restaurants_google, latin_restaurants_yelp], ignore_index=True)
        latin_restaurants = latin_restaurants.rename(columns={'category': 'category_name'})
        print('Restaurantes latinos de Google y Yelp concatenados')

        ### Extraccion de las categorias de los restaurantes
        categories = latin_restaurants[['category_name']]
        categories = categories.drop_duplicates(subset='category_name')
        categories['category_id'] = categories['category_name'].astype('category').cat.codes + 1
        categories = categories.reset_index(drop=True)
        categories = categories.reindex(columns=['category_id', 'category_name'])
        print('Categorias de restaurantes extraidas')

        # Generación de los IDs de reviews
        reviews_google['review_id'] = reviews_google['restaurant_id'].str[:10] + '_' + reviews_google['user_id'].str[:10]
        print('ID de reviews de Google creados')

        ### Orden de las columnas en los dataframe de reviews
        nuevo_orden_reviews_google = ['review_id', 'restaurant_id', 'user_id', 'rating', 'text', 'name']
        nuevo_orden_reviews = ['review_id', 'restaurant_id', 'user_id', 'rating', 'text']
        reviews_google = reviews_google.reindex(columns=nuevo_orden_reviews_google)
        reviews_yelp = reviews_yelp.reindex(columns=nuevo_orden_reviews)
        print('Orden de columnas en los dataframe de reviews asignado')

        # Concatenación de los usuarios
        users_google = reviews_google[['user_id', 'name']].drop_duplicates(subset='user_id').rename(columns={'name': 'user_name'})
        reviews_google.drop(columns=['name'], inplace=True)
        users_yelp = users_yelp.rename(columns={'name': 'user_name'})
        users = pd.concat([users_google, users_yelp], ignore_index=True)
        print('Usuarios concatenados')

        # Concatenación de las reviews
        reviews = pd.concat([reviews_google, reviews_yelp], ignore_index=True)
        print('Reviews concatenadas')

        # Merge de los estados y census
        states = pd.merge(census, states, on='state_name', how='inner')
        states = states.rename(columns={'total_population_y': 'total_population', 
                                        'hispanic_population_y': 'hispanic_population', 
                                        'avg_income_y': 'avg_income'})
        print('Data de los estados unida')

        #### Nuevo orden para las columnas
        nuevo_orden_states = ['state_id', 'state_name', 'abbreviation', 'total_population', 'hispanic_population', 'avg_income']
        states = states.reindex(columns=nuevo_orden_states)
        print('Orden de columnas en la data de los estados asignado')

        #### Normalizacion de los datos numericos
        states['avg_income'] = states['avg_income'].round(2)
        states['hispanic_population'] = states['hispanic_population'].round().astype(int)
        print('Datos numericos normalizados')

        ## Relacion de restaurantes con categorias
        latin_restaurants = pd.merge(latin_restaurants, categories, on='category_name', how='inner')
        latin_restaurants = latin_restaurants.drop(columns=['category_name'])
        print('Relacion de restaurantes con categorias creada')        

        ## Relacion de restaurantes con estados
        latin_restaurants = pd.merge(latin_restaurants, states, on='abbreviation', how='inner')
        latin_restaurants = latin_restaurants.drop(columns=['abbreviation', 'state_name', 'total_population', 'hispanic_population', 'avg_income'])
        print('Relacion de restaurantes con estados creada')

        # Guardar los resultados en GCS
        save_to_gcs(latin_restaurants, BUCKET_NAME, output_folder, 'restaurants_processed.parquet')
        save_to_gcs(reviews, BUCKET_NAME, output_folder, 'reviews_processed.parquet')
        save_to_gcs(users, BUCKET_NAME, output_folder, 'users_processed.parquet')
        save_to_gcs(states, BUCKET_NAME, output_folder, 'states_processed.parquet')
        save_to_gcs(categories, BUCKET_NAME, output_folder, 'categories_processed.parquet')

        return 'Data processed and saved successfully.'

    except Exception as e:
        print(f'Error processing data: {e}')
        return f'Error processing data: {e}'
