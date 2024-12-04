import sys
import os


from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta


from data_extraction.usa_spending_extraction_script import extract_usa_spending_data
from data_processing.usa_spending_processing_script import process_usa_spending_data
from data_ingestion.usa_spending_data_ingestion import ingest_usa_spending_data

# Define the Airflow DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=0),
}



usa_spending_dag = DAG(
    'ETL_usa_spending',
    default_args=default_args,
    description='A DAG to extract and process USA performance reports and upload them to S3',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    tags=[ 'USASpending'],
)



# USA spending tasks
extract_usa_spending_data_task = PythonOperator(
    task_id='extract_usa_spending_data_task',
    python_callable=extract_usa_spending_data,
    dag=usa_spending_dag,
)

process_usa_spending_data_task = PythonOperator(
    task_id='process_usa_spending_data_task',
    python_callable=process_usa_spending_data,
    dag=usa_spending_dag,
)
ingest_usa_spending_data_task = PythonOperator(
    task_id='ingest_usa_spending_data_task',
    python_callable=ingest_usa_spending_data,
    dag=usa_spending_dag,
)

extract_usa_spending_data_task >> process_usa_spending_data_task >> ingest_usa_spending_data_task