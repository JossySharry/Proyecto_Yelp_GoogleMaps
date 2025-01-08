# <h1 align="center">**Documentación de ETL y Orquestación en Google Cloud**</h1>

Esta carpeta contiene las funciones implementadas en Google Cloud Functions (GCF) y el flujo de trabajo automatizado diseñado para gestionar el ciclo de vida completo de los datos en la nube. El objetivo principal es procesar, transformar y almacenar datos de restaurantes para identificar riesgos y oportunidades de inversión.

## ```Descripción del Proceso```

El flujo de trabajo se divide en dos etapas principales:

```Carga inicial de datos crudos:```

Un programador de tareas se encarga de ejecutar la función de extracción a una hora previamente configurada. Esta función almacena los datos crudos en un bucket de Google Cloud Storage (GCS).

```Automatización del flujo mediante Scheduler:```

Una vez realizada la carga inicial, Cloud Scheduler ejecuta la función orchestrator, que inicia la ejecución secuencial del resto de las funciones en GCF:

```process_data y process_data_api:``` Transforman los datos almacenados en GCS.

```tables_bigquery y tables_bigquery_api:``` Cargan los datos procesados en tablas de BigQuery.

```notificacion:``` Envía una notificación una vez finalizado el proceso.

Este esquema permite la automatización total del ciclo de vida del dato en la nube, sin necesidad de intervención manual.

```Funciones en Google Cloud Functions```

1. orchestrator

    Orquesta la ejecución secuencial de las demás funciones.

2. extraccion_api

    Extrae los datos desde una API y los almacena en el bucket correspondiente en GCS.

3. process_data

    Realiza las transformaciones necesarias en los datos provenientes de fuentes locales.

4. process_data_api

    Transforma los datos provenientes de la API.

5. tables_bigquery

    Crea y carga tablas en BigQuery con los datos procesados desde fuentes locales.

6. tables_bigquery_api

    Crea y carga tablas en BigQuery con los datos procesados desde la API.

7. notificacion

    Envía una notificación (por ejemplo, un correo electrónico) indicando la finalización del ciclo de datos.


## ```Despliegue de funciones en Google Cloud Console:```

1. Accede a la consola de Google Cloud Platform y ve a la sección Cloud Functions.

2. Crea una nueva función haciendo clic en Create Function.

3. Sube los archivos necesarios (main.py y requirements.txt) en el editor que proporciona la consola.

4. Configura los parámetros de ejecución, como el Runtime (Python 3.10), el Trigger HTTP, y el Nombre del entry point (debe coincidir con el nombre de la función principal en tu archivo main.py).

5. Haz clic en Deploy para implementar la función.

6. Configura el Scheduler:

    Define los triggers de Cloud Scheduler para ejecutar la función orchestrator automáticamente.

    Configura el programador de tareas para iniciar la carga inicial.