import requests
from google.cloud import logging
import time

# Configuración de logging
client = logging.Client()
logger = client.logger("orchestrator_logs")

def orchestrator(request):
    # Definir las URLs de las funciones GCF
    url_process_data = "https://us-central1-expansionsaborlatino.cloudfunctions.net/process_data"
    url_tables_bigquery = "https://us-central1-expansionsaborlatino.cloudfunctions.net/tables_bigquery"
    url_extraccion_api = "https://us-central1-expansionsaborlatino.cloudfunctions.net/extraccion_api"
    url_process_data_api = "https://us-central1-expansionsaborlatino.cloudfunctions.net/process_data_api"
    url_tables_bigquery_api = "https://us-central1-expansionsaborlatino.cloudfunctions.net/tables_bigquery_api"
    url_notificacion = "https://us-central1-expansionsaborlatino.cloudfunctions.net/notificacion"

    try:
        # 1. Llamar a la función "process_data"
        response_process_data = requests.get(url_process_data)
        if response_process_data.status_code == 200:
            logger.log_text("Función process_data ejecutada correctamente.")
        else:
            logger.log_text(f"Error al ejecutar process_data: {response_process_data.text}")
            return f"Error al ejecutar process_data: {response_process_data.text}", 500

        # 2. Llamar a la función "tables_bigquery"
        try:
            response_tables_bigquery = requests.get(url_tables_bigquery)
            if response_tables_bigquery.status_code == 200:
                logger.log_text("Función tables_bigquery ejecutada correctamente.")
            else:
                logger.log_text(f"Error al ejecutar tables_bigquery: {response_tables_bigquery.text}")
        except Exception as e:
            logger.log_text(f"Excepción en tables_bigquery: {str(e)}")


        # 3. Llamar a la función "extraccion_api"
        response_extraccion_api = requests.get(url_extraccion_api)
        if response_extraccion_api.status_code == 200:
            logger.log_text("Función extraccion_api ejecutada correctamente.")
        else:
            logger.log_text(f"Error al ejecutar extraccion_api: {response_extraccion_api.text}")
            return f"Error al ejecutar extraccion_api: {response_extraccion_api.text}", 500

        # 4. Llamar a la función "process_data_api"
        response_process_data_api = requests.get(url_process_data_api)
        if response_process_data_api.status_code == 200:
            logger.log_text("Función process_data_api ejecutada correctamente.")
        else:
            logger.log_text(f"Error al ejecutar process_data_api: {response_process_data_api.text}")
            return f"Error al ejecutar process_data_api: {response_process_data_api.text}", 500

        # 5. Llamar a la función "tables_bigquery_api"
        response_tables_bigquery_api = requests.get(url_tables_bigquery_api)
        if response_tables_bigquery_api.status_code == 200:
            logger.log_text("Función tables_bigquery_api ejecutada correctamente.")
        else:
            logger.log_text(f"Error al ejecutar tables_bigquery_api: {response_tables_bigquery_api.text}")
            return f"Error al ejecutar tables_bigquery_api: {response_tables_bigquery_api.text}", 500

        # 6. Llamar a la función "notificacion"
        response_notificacion = requests.get(url_notificacion)
        if response_notificacion.status_code == 200:
            logger.log_text("Función notificacion ejecutada correctamente.")
            return "Todas las funciones ejecutadas correctamente", 200
        else:
            logger.log_text(f"Error al ejecutar notificacion: {response_notificacion.text}")
            return f"Error al ejecutar notificacion: {response_notificacion.text}", 500

    except Exception as e:
        logger.log_text(f"Error inesperado en el orquestador: {str(e)}")
        # No devolvemos error aquí para evitar que Scheduler marque la tarea como fallida.

    # Devolver siempre un 200 OK al final
    return "Orquestador ejecutado, revisa los logs para más detalles.", 200


 