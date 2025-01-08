from google.cloud import bigquery

def create_table(project_id, dataset_id, bucket_name, file_name, table_name):
    """Crea una tabla en BigQuery a partir de un archivo Parquet en Google Cloud Storage.

    Args:
        project_id: ID del proyecto de Google Cloud.
        dataset_id: ID del dataset en BigQuery.
        bucket_name: Nombre del bucket en Google Cloud Storage.
        file_name: Nombre del archivo en Google Cloud Storage.
        table_name: Nombre de la tabla a crear en BigQuery.
    """

    client = bigquery.Client(project=project_id)
    dataset_ref = f"{project_id}.{dataset_id}"
    table_ref = f"{dataset_ref}.{table_name}"


    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )

    uri = get_gcs_uri(bucket_name, file_name)

    load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
    load_job.result()  # Espera a que el trabajo se complete
    print(f"Tabla '{table_name}' creada exitosamente.")

def get_gcs_uri(bucket_name, file_name):
    return f"gs://{bucket_name}/processed/{file_name}"


project_id = "expansionsaborlatino"
dataset_id = "Saborlatino"
bucket_name = "saborlatino"
file_names = ["restaurants_processed.parquet", "reviews_processed.parquet", "users_processed.parquet", "states_processed.parquet", "categories_processed.parquet"]

# Crear las tablas
#for file_name in file_names:
 #   table_name = file_name.replace("_processed.parquet", "")
  #  create_table(project_id, dataset_id, bucket_name, file_name, table_name)

for file_name in file_names:
    try:
        table_name = file_name.replace("_processed.parquet", "")
        create_table(project_id, dataset_id, bucket_name, file_name, table_name)
    except Exception as e:
        print(f"Error al procesar {file_name}: {e}")
        # Continúa con las demás tablas
