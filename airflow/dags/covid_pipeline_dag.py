import sys
import os

# Add scripts directory to Python path
scripts_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), '/opt/airflow/scripts')
sys.path.append(scripts_dir)


from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from extract import extract_data
from transform import transform_data  
from load import load_data


CONFIG = {
    'raw_data_path': '/opt/airflow/data/raw',
    'processed_data_path': '/opt/airflow/data/processed',
    'log_file': '/opt/airflow/pipeline.log',
    'postgres_conn_id' : 'covid_data',

}

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'covid19_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for COVID-19 data',
    # schedule='@daily',
    schedule=timedelta(minutes=5),
    catchup=False
)


extract_task = PythonOperator(
    task_id='extract_covid_data',
    python_callable=extract_data,
    op_kwargs={'config': CONFIG},
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_covid_data',
    python_callable=transform_data,
    op_kwargs={'config': CONFIG},
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_covid_data',
    python_callable=load_data,
    op_kwargs={'config': CONFIG},
    dag=dag,
)

# Define the task dependencies
extract_task >> transform_task >> load_task