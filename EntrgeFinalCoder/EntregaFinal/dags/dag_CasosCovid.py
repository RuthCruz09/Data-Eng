import logging
from airflow.utils.email import send_email
from airflow.models import TaskInstance
from datetime import datetime
from airflow.models import DAG, Variable
from scripts.etl_CasosCovid import * 
from datetime import datetime as dt, timedelta
from email.mime.multipart import MIMEMultipart
from datetime import datetime as dt, timedelta
from email.mime.text import MIMEText
from sqlalchemy import create_engine
from airflow.operators.python import PythonOperator 
from functools import partial
import smtplib
from airflow.utils.dates import days_ago
from datetime import datetime
from airflow import DAG
from datetime import timedelta
import os
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
dag_path = os.getcwd() 


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 9, 10),
    'email_on_failure': True,
    'email_on_retry': False, 
    'email': 'ruthn.cruz09@gmail.com'
}

with DAG( 
    dag_id='CasosCovid',
    schedule_interval="* * * * *",
    catchup=False,
    default_args=default_args
) as dag:
    task_1 = PythonOperator(
        task_id='Get_Api_covid',
        python_callable=get_covid_data,
        op_kwargs={'base_url': base_url, 'params': params},
        provide_context=True,
        retries=3,
        retry_delay=timedelta(minutes=5),
        email_on_failure=True
    )

    task_2 = PythonOperator(
        task_id="conexion_redshift",
        python_callable=connect_to_db,
        op_args=["config/config_file.ini"], 
        retries=3,
        retry_delay=timedelta(minutes=5),
        email_on_failure=True
    )

    task_3 = PythonOperator(
        task_id='insert_data_task',
        python_callable=carga_to_sql,
        retries=3,
        retry_delay=timedelta(minutes=5),
        email_on_failure=True
    )
    
    task_4 = PythonOperator(
        task_id='notify_task_status',
        python_callable=notify_task_status,
        op_args=['dag_smtp_email_callback', '{{ execution_date }}'],  
    )
# orden de ejecucion de las tareas
task_1 >> task_2 >> task_3 >> task_4

