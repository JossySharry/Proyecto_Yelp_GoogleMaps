import time
import pandas as pd
from functions import upload_dataframe_to_gcs

### Carga de data
negocios_google = pd.read_parquet(r'C:\Users\Operador\Desktop\DataScience\DataScienceHenry\PF\Datasets_limpios\sitios_google_original.parquet')
print('Sitios originales de google cargados')

negocios_google = negocios_google.dropna(subset=['category'])
sitios_original_exploded = negocios_google.explode('category')
print('Sitios originales de google cargados')

# Aplicando filtro para determinar los restaurantes
filtro_restaurant = sitios_original_exploded['category'].str.contains('restaurant', case=False)
google_restaurants = sitios_original_exploded[filtro_restaurant]
print('Restaurantes de google filtrados')

# Combinar gentilicios con las categorías iniciales
latin_categories_keywords = ['mexican', 'latin', 'brazilian', 'colombian', 'cuban', 
    'peruvian', 'puerto rican', 'caribbean', 'burrito', 'taco', 'pozole', 'South American', 'Oaxacan', 'argentine', 'argentinian', 'bolivian', 'chilean', 'colombian', 'costa rican', 
    'cuban', 'dominican', 'ecuadorian', 'guatemalan', 'honduran', 'mexican', 
    'nicaraguan', 'panamanian', 'paraguayan', 'peruvian', 'puerto rican', 
    'salvadoran', 'uruguayan', 'venezuelan'
]

# Crear el patrón de búsqueda
pattern_categories = '|'.join(latin_categories_keywords)

# Aplicar el filtro
latin_restaurants_google = google_restaurants[google_restaurants['category'].str.contains(pattern_categories, case=False, na=False)]
print('Restaurantes latinos de google filtrados')

del negocios_google

### Carga de data
latin_restaurants_yelp = pd.read_parquet(r'C:\Users\Operador\Desktop\DataScience\DataScienceHenry\PF\Datasets_limpios\restaurantes_latinos_yelp.parquet')
print('Restaurantes latinos de yelp cargados')

### Configuracion de variables
BUCKET_NAME = "saborlatino"
DESTINATION_BLOB_NAME = "raw/latin_restaurants_yelp.parquet"

### Intentar subir el DataFrame al bucket de Cloud Storage
MAX_RETRIES = 3
RETRY_DELAY = 5  # segundos

for attempt in range(MAX_RETRIES):
    try:
        # Tu código de carga actual
        upload_dataframe_to_gcs(BUCKET_NAME, latin_restaurants_yelp, DESTINATION_BLOB_NAME, 'parquet')
        break
    except Exception as e:
        print(f"Error en el intento {attempt+1}: {e}")
        if attempt < MAX_RETRIES - 1:
            print(f"Esperando {RETRY_DELAY} segundos antes de reintentar...")
            time.sleep(RETRY_DELAY)

del latin_restaurants_yelp

### Carga de las reviews
reviews_arizona     = pd.read_parquet(r'C:\Users\Operador\Desktop\DataScience\DataScienceHenry\PF\Datasets_limpios\reviews_arizona.parquet')
print('Reviews de Arizona cargadas')

### Union de todos los restaurantes latinos con las reviews (ARIZONA)
reviews_az = pd.merge(latin_restaurants_google, reviews_arizona, on='gmap_id', how='inner')
reviews_az.drop(columns=['name_x', 'address', 'description', 'latitude', 'longitude', 'category', 'avg_rating', 'num_of_reviews', 'price', 'hours','MISC','state', 'relative_results', 'url'], inplace=True)
reviews_az.rename(columns={'name_y':'name'}, inplace=True)

latin_restaurants_google_az = pd.merge(latin_restaurants_google, reviews_arizona, on='gmap_id', how='inner')
latin_restaurants_google_az['abbreviation'] = 'AZ'
latin_restaurants_google_az = latin_restaurants_google_az.drop_duplicates(subset='gmap_id')
latin_restaurants_google_az.drop(columns=['user_id', 'name_y','rating', 'text', 'time', 'pics', 'resp'], inplace=True)
latin_restaurants_google_az.rename(columns={'name_x':'name'}, inplace=True)

del reviews_arizona

print('Reviews de Arizona filtradas por los restaurantes latinos de google')

reviews_california  = pd.read_parquet(r'C:\Users\Operador\Desktop\DataScience\DataScienceHenry\PF\Datasets_limpios\reviews_california.parquet')

### Union de todos los restaurantes latinos con las reviews (CALIFORNIA)
reviews_ca = pd.merge(latin_restaurants_google, reviews_california, on='gmap_id', how='inner')
reviews_ca.drop(columns=['name_x', 'address', 'description', 'latitude', 'longitude', 'category', 'avg_rating', 'num_of_reviews', 'price', 'hours','MISC','state', 'relative_results', 'url'], inplace=True)
reviews_ca.rename(columns={'name_y':'name'}, inplace=True)

latin_restaurants_google_ca = pd.merge(latin_restaurants_google, reviews_california, on='gmap_id', how='inner')
latin_restaurants_google_ca['abbreviation'] = 'CA'
latin_restaurants_google_ca = latin_restaurants_google_ca.drop_duplicates(subset='gmap_id')
latin_restaurants_google_ca.drop(columns=['user_id', 'name_y','rating', 'text', 'time', 'pics', 'resp'], inplace=True)
latin_restaurants_google_ca.rename(columns={'name_x':'name'}, inplace=True)

del reviews_california

print('Reviews de California filtradas por los restaurantes latinos de google')

reviews_florida = pd.read_parquet(r'C:\Users\Operador\Desktop\DataScience\DataScienceHenry\PF\Datasets_limpios\reviews_florida.parquet')

### Union de todos los restaurantes latinos con las reviews (FLORIDA)
reviews_fl = pd.merge(latin_restaurants_google, reviews_florida, on='gmap_id', how='inner')
reviews_fl.drop(columns=['name_x', 'address', 'description', 'latitude', 'longitude', 'category', 'avg_rating', 'num_of_reviews', 'price', 'hours','MISC','state', 'relative_results', 'url'], inplace=True)
reviews_fl.rename(columns={'name_y':'name'}, inplace=True)

latin_restaurants_google_fl = pd.merge(latin_restaurants_google, reviews_florida, on='gmap_id', how='inner')
latin_restaurants_google_fl['abbreviation'] = 'FL'
latin_restaurants_google_fl = latin_restaurants_google_fl.drop_duplicates(subset='gmap_id')
latin_restaurants_google_fl.drop(columns=['user_id', 'name_y','rating', 'text', 'time', 'pics', 'resp'], inplace=True)
latin_restaurants_google_fl.rename(columns={'name_x':'name'}, inplace=True)

del reviews_florida

print('Reviews de Florida filtradas por los restaurantes latinos de google')

reviews_texas = pd.read_parquet(r'C:\Users\Operador\Desktop\DataScience\DataScienceHenry\PF\Datasets_limpios\reviews_texas.parquet')

### Union de todos los restaurantes latinos con las reviews (TEXAS)
reviews_tx = pd.merge(latin_restaurants_google, reviews_texas, on='gmap_id', how='inner')
reviews_tx.drop(columns=['name_x', 'address', 'description', 'latitude', 'longitude', 'category', 'avg_rating', 'num_of_reviews', 'price', 'hours','MISC','state', 'relative_results', 'url'], inplace=True)
reviews_tx.rename(columns={'name_y':'name'}, inplace=True)

latin_restaurants_google_tx = pd.merge(latin_restaurants_google, reviews_texas, on='gmap_id', how='inner')
latin_restaurants_google_tx['abbreviation'] = 'TX'
latin_restaurants_google_tx = latin_restaurants_google_tx.drop_duplicates(subset='gmap_id')
latin_restaurants_google_tx.drop(columns=['user_id', 'name_y','rating', 'text', 'time', 'pics', 'resp'], inplace=True)
latin_restaurants_google_tx.rename(columns={'name_x':'name'}, inplace=True)

del reviews_texas

print('Reviews de Texas filtradas por los restaurantes latinos de google')

reviews_illinois    = pd.read_parquet(r'C:\Users\Operador\Desktop\DataScience\DataScienceHenry\PF\Datasets_limpios\reviews_illinois.parquet')

### Union de todos los restaurantes latinos con las reviews (Illinois)
reviews_il = pd.merge(latin_restaurants_google, reviews_illinois, on='gmap_id', how='inner')
reviews_il.drop(columns=['name_x', 'address', 'description', 'latitude', 'longitude', 'category', 'avg_rating', 'num_of_reviews', 'price', 'hours','MISC','state', 'relative_results', 'url'], inplace=True)
reviews_il.rename(columns={'name_y':'name'}, inplace=True)

latin_restaurants_google_il = pd.merge(latin_restaurants_google, reviews_illinois, on='gmap_id', how='inner')
latin_restaurants_google_il['abbreviation'] = 'IL'
latin_restaurants_google_il = latin_restaurants_google_il.drop_duplicates(subset='gmap_id')
latin_restaurants_google_il.drop(columns=['user_id', 'name_y','rating', 'text', 'time', 'pics', 'resp'], inplace=True)
latin_restaurants_google_il.rename(columns={'name_x':'name'}, inplace=True)

del reviews_illinois

print('Reviews de Illinois filtradas por los restaurantes latinos de google')

reviews_newyork  = pd.read_parquet(r'C:\Users\Operador\Desktop\DataScience\DataScienceHenry\PF\Datasets_limpios\reviews_newyork.parquet')

### Union de todos los restaurantes latinos con las reviews (New York)
reviews_ny = pd.merge(latin_restaurants_google, reviews_newyork, on='gmap_id', how='inner')
reviews_ny.drop(columns=['name_x', 'address', 'description', 'latitude', 'longitude', 'category', 'avg_rating', 'num_of_reviews', 'price', 'hours','MISC','state', 'relative_results', 'url'], inplace=True)
reviews_ny.rename(columns={'name_y':'name'}, inplace=True)

latin_restaurants_google_ny = pd.merge(latin_restaurants_google, reviews_newyork, on='gmap_id', how='inner')
latin_restaurants_google_ny['abbreviation'] = 'NY'
latin_restaurants_google_ny = latin_restaurants_google_ny.drop_duplicates(subset='gmap_id')
latin_restaurants_google_ny.drop(columns=['user_id', 'name_y','rating', 'text', 'time', 'pics', 'resp'], inplace=True)
latin_restaurants_google_ny.rename(columns={'name_x':'name'}, inplace=True)

del reviews_newyork

print('Reviews de New York filtradas por los restaurantes latinos de google')

latin_restaurants_google = pd.concat([latin_restaurants_google_az, latin_restaurants_google_ca, latin_restaurants_google_fl, latin_restaurants_google_tx, latin_restaurants_google_il, latin_restaurants_google_ny], ignore_index=True)
print('Restaurantes latinos de google concatenados')

## Negocio Google
### Configuracion de variables
BUCKET_NAME = "saborlatino"
DESTINATION_BLOB_NAME = "raw/latin_restaurants_google.parquet"

### Intentar subir el DataFrame al bucket de Cloud Storage
MAX_RETRIES = 3
RETRY_DELAY = 5  # segundos

for attempt in range(MAX_RETRIES):
    try:
        # Tu código de carga actual
        upload_dataframe_to_gcs(BUCKET_NAME, latin_restaurants_google, DESTINATION_BLOB_NAME, 'parquet')
        break
    except Exception as e:
        print(f"Error en el intento {attempt+1}: {e}")
        if attempt < MAX_RETRIES - 1:
            print(f"Esperando {RETRY_DELAY} segundos antes de reintentar...")
            time.sleep(RETRY_DELAY)

del latin_restaurants_google

### Concatenacion de las reviews
reviews_google = pd.concat([reviews_az, reviews_ca, reviews_fl, reviews_tx, reviews_il, reviews_ny], ignore_index=True)
print('Reviews de google concatenadas')

# Reviews
### Configuracion de variables
BUCKET_NAME = "saborlatino"
DESTINATION_BLOB_NAME = "raw/reviews_google.parquet"

### Intentar subir el DataFrame al bucket de Cloud Storage
MAX_RETRIES = 3
RETRY_DELAY = 5  # segundos

for attempt in range(MAX_RETRIES):
    try:
        # Tu código de carga actual
        upload_dataframe_to_gcs(BUCKET_NAME, reviews_google, DESTINATION_BLOB_NAME, 'parquet')
        break
    except Exception as e:
        print(f"Error en el intento {attempt+1}: {e}")
        if attempt < MAX_RETRIES - 1:
            print(f"Esperando {RETRY_DELAY} segundos antes de reintentar...")
            time.sleep(RETRY_DELAY)

del reviews_google

### Carga de las reviews de yelp
reviews_yelp = pd.read_parquet(r'C:\Users\Operador\Desktop\DataScience\DataScienceHenry\PF\Datasets_limpios\reviews_yelp.parquet')

## Reviews Yelp
### Configuracion de variables
BUCKET_NAME = "saborlatino"
DESTINATION_BLOB_NAME = "raw/reviews_yelp.parquet"

### Intentar subir el DataFrame al bucket de Cloud Storage
MAX_RETRIES = 3
RETRY_DELAY = 5  # segundos

for attempt in range(MAX_RETRIES):
    try:
        # Tu código de carga actual
        upload_dataframe_to_gcs(BUCKET_NAME, reviews_yelp, DESTINATION_BLOB_NAME, 'parquet')
        break
    except Exception as e:
        print(f"Error en el intento {attempt+1}: {e}")
        if attempt < MAX_RETRIES - 1:
            print(f"Esperando {RETRY_DELAY} segundos antes de reintentar...")
            time.sleep(RETRY_DELAY)

del reviews_yelp

### Carga de usuarios de yelp
users_yelp = pd.read_parquet(r'C:\Users\Operador\Desktop\DataScience\DataScienceHenry\PF\Datasets_limpios\users_yelp.parquetet')

## Users Yelp
### Configuracion de variables
BUCKET_NAME = "saborlatino"
DESTINATION_BLOB_NAME = "raw/users_yelp.parquet"

### Intentar subir el DataFrame al bucket de Cloud Storage
MAX_RETRIES = 3
RETRY_DELAY = 5  # segundos

for attempt in range(MAX_RETRIES):
    try:
        # Tu código de carga actual
        upload_dataframe_to_gcs(BUCKET_NAME, users_yelp, DESTINATION_BLOB_NAME, 'parquet')
        break
    except Exception as e:
        print(f"Error en el intento {attempt+1}: {e}")
        if attempt < MAX_RETRIES - 1:
            print(f"Esperando {RETRY_DELAY} segundos antes de reintentar...")
            time.sleep(RETRY_DELAY)

del users_yelp

### Carga de censo y estados
census = pd.read_csv(r'C:\Users\Operador\Desktop\DataScience\DataScienceHenry\PF\Datasets_limpios\censo.csv')
states = pd.read_csv(r'C:\Users\Operador\Desktop\DataScience\DataScienceHenry\PF\Datasets_limpios\states.csv')
print('Data de censo y estados cargada')

## States Y Censo
### Configuracion de variables
BUCKET_NAME = "saborlatino"
DESTINATION_BLOB_NAME = "raw/census.parquet"

### Intentar subir el DataFrame al bucket de Cloud Storage
MAX_RETRIES = 3
RETRY_DELAY = 5  # segundos

for attempt in range(MAX_RETRIES):
    try:
        # Tu código de carga actual
        upload_dataframe_to_gcs(BUCKET_NAME, census, DESTINATION_BLOB_NAME, 'parquet')
        break
    except Exception as e:
        print(f"Error en el intento {attempt+1}: {e}")
        if attempt < MAX_RETRIES - 1:
            print(f"Esperando {RETRY_DELAY} segundos antes de reintentar...")
            time.sleep(RETRY_DELAY)

del census

### Configuracion de variables
BUCKET_NAME = "saborlatino"
DESTINATION_BLOB_NAME = "raw/states.parquet"

### Intentar subir el DataFrame al bucket de Cloud Storage
MAX_RETRIES = 3
RETRY_DELAY = 5  # segundos

for attempt in range(MAX_RETRIES):
    try:
        # Tu código de carga actual
        upload_dataframe_to_gcs(BUCKET_NAME, states, DESTINATION_BLOB_NAME, 'parquet')
        break
    except Exception as e:
        print(f"Error en el intento {attempt+1}: {e}")
        if attempt < MAX_RETRIES - 1:
            print(f"Esperando {RETRY_DELAY} segundos antes de reintentar...")
            time.sleep(RETRY_DELAY)

del states

print('Carga de datos a Cloud Storage finalizada')