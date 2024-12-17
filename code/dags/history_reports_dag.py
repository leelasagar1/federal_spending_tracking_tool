import sys
import os


from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

from data_extraction.web_scraper import     scrape_and_upload_history_reports

from data_processing.history_reports_processing_script import process_history_reports

from data_ingestion.history_reports_data_ingestion import ingest_history_reports
# Define the Airflow DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=0),
}


history_reports_dag = DAG(
    'ETL_history_reports',
    default_args=default_args,
    description='A DAG to scrape history reports and upload them to S3',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    tags=['webscraping', 's3', 'hisotry_reports'],
)






# History reports tasks

fetch_history_reports_task = PythonOperator(
    task_id='fetch_history_reports_task',
    python_callable=scrape_and_upload_history_reports,
    dag=history_reports_dag,
)
process_history_reports_task = PythonOperator(
    task_id='process_history_reports_task',
    python_callable=process_history_reports,
    dag=history_reports_dag,
)
ingest_history_reports_task = PythonOperator(
    task_id='ingest_history_reports_task',
    python_callable=ingest_history_reports,
    dag=history_reports_dag,
)



fetch_history_reports_task  >> process_history_reports_task >> ingest_history_reports_task
