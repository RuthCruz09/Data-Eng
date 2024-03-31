
from airflow.models import TaskInstance
import os
from psycopg2 import extras
from airflow.utils.state import State
import psycopg2.extras
import configparser
from email.mime.multipart import MIMEMultipart
from datetime import datetime as dt, timedelta
import psycopg2
from sqlalchemy import create_engine
from airflow.operators.python import PythonOperator 
from functools import partial
from airflow.utils.dates import days_ago
from datetime import timedelta
from email.mime.text import MIMEText
from psycopg2.extras import execute_values
from configparser import ConfigParser
import requests
from pandas import json_normalize, to_datetime, concat
import logging
import smtplib
import pandas as pd
from airflow.models import DAG, Variable
import json
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.utils.email import send_email
from airflow.models import TaskInstance

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

base_url = "https://api.covidactnow.org/v2/counties.json?apiKey=14c3113b02cb4cca87736b9f60a1f4e7"
params = {
    'lastUpdatedDate': (dt.utcnow() - timedelta(hours=20)).strftime('%Y-%m-%dT%H:00:00'),
    'end': (dt.utcnow() - timedelta(hours=20)).strftime('%Y-%m-%dT%H:59:59')
}


def get_covid_data(base_url, params):
    try:
        covidurl = f"{base_url}/{params['lastUpdatedDate']}/{params['end']}"
        logging.info(f"Obteniendo datos de {covidurl}...")
        logging.info(f"Parámetros: {params}")
        response = requests.get(covidurl)
        response.raise_for_status()  # Levanta una excepción si hay un error en la respuesta HTTP.
        logging.info(response.url)
        logging.info("Datos obtenidos exitosamente... Procesando datos...")
        data = response.json()
        data = data["data"]
        df = json_normalize(data)
        
        # Lista de columnas específicas que deseas mantener
        columnas_especificas = ['fips','country','state','county','population','lastUpdatedDate','actuals.cases','actuals.deaths','actuals.icuBeds.capacity','actuals.icuBeds.currentUsageTotal','actuals.icuBeds.currentUsageCovid']  # Reemplaza con las columnas que necesitas
        
        # Seleccionar solo las columnas específicas del DataFrame
        df = df.loc[:, columnas_especificas]
        
        logging.info("Datos procesados exitosamente")
        return df

    except requests.exceptions.RequestException as e:
        logging.error(f"La petición a {covidurl} ha fallado: {e}")
        return None

    except json.JSONDecodeError:
        logging.error(f"Respuesta en formato incorrecto de {covidurl}")
        return None
        
    except Exception as e:
        logging.exception(f"Error al obtener datos de {covidurl}: {e}")
        return None
    

def connect_to_db(config_file):
    

    try:
        # Lee el archivo .ini
        config = configparser.ConfigParser()
        config.read(config_file)

        # Extrae los valores de conexión de la sección [redshift1]
        redshift_credentials = {
            'dbname': config.get('redshift1', 'dbname'),
            'user': config.get('redshift1', 'user'),
            'password': config.get('redshift1', 'pwd'),
            'host': config.get('redshift1', 'host'),
            'port': config.get('redshift1', 'port')
        }

        # Conecta con Redshift
        conn = psycopg2.connect(**redshift_credentials)
        print("Conexión a la base de datos establecida exitosamente")
        return conn
        
    except Exception as e:
        print(f"Error al conectarse a la base de datos: {e}")
        return None



def carga_to_sql(df, stg_covidfinal, engine, stg_population):
    try:
        with engine.connect() as conn, conn.begin():
            conn.execute('TRUNCATE TABLE {}'.format(stg_covidfinal))
            df.to_sql(
                stg_covidfinal,
                schema='ruthn_cruz09_coderhouse',
                con=engine,
                index=False,
                method='multi',
                if_exists='append'
            )

            conn.execute("""
                        MERGE INTO covidfinal 
                        USING {} AS stg
                        ON covidfinal.fips = stg.fips
                        AND covidfinal.country = stg.country
                        AND covidfinal.state = stg.state
                        AND covidfinal.county = stg.county
                        AND covidfinal.lastUpdatedDate = stg.lastUpdatedDate
                        AND covidfinal."actuals.cases" = stg."actuals.cases"
                        AND covidfinal."actuals.deaths" = stg."actuals.deaths"
                        AND covidfinal."actuals.icuBeds.capacity" = stg."actuals.icuBeds.capacity"
                        WHEN MATCHED THEN
                        UPDATE SET population = stg.{}
                        WHEN NOT MATCHED THEN
                        INSERT (fips, country, state, county, population, lastUpdatedDate,
                                "actuals.cases", "actuals.deaths", "actuals.icuBeds.capacity", 
                                "actuals.icuBeds.currentUsageTotal", "actuals.icuBeds.currentUsageCovid")
                        VALUES (stg.fips, stg.country, stg.state, stg.county, stg.{}, stg.lastUpdatedDate,
                                stg."actuals.cases", stg."actuals.deaths", stg."actuals.icuBeds.capacity", 
                                stg."actuals.icuBeds.currentUsageTotal", stg."actuals.icuBeds.currentUsageCovid")
                                ;
            """.format(stg_population, stg_population, stg_population))

    except Exception as e:
        logging.error(f"Error al cargar datos en la base de datos: {e}")





def notify_task_status(dag_id, execution_date):
    # Obtiene todas las instancias de tarea para el DAG y la fecha de ejecución
    task_instances = TaskInstance.find(dag_id=dag_id, execution_date=execution_date)
    
    # Crea el cuerpo del correo electrónico
    email_body = "Estado de las tareas en el DAG:\n\n"
    for task_instance in task_instances:
        if task_instance.state != 'success':
            email_body += f"Task ID: {task_instance.task_id}, Estado: {task_instance.state}\n"
    
    # Si no hay tareas fallidas, no envia correo electrónico
    if 'failed' not in [ti.state for ti in task_instances]:
        return
    
    # Configura el asunto del correo electrónico
    subject = f"Estado de las tareas en el DAG {dag_id} - {execution_date}"
    
    # Dirección de correo electrónico a la que se enviará la notificación
    recipient = 'ruthn.cruz09@gmail.com'
    
    # Enviar correo electrónico
    send_email(recipient, subject, email_body)