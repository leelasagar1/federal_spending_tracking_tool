import sys
import os


from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta



from data_processing.quarterly_performance_reports_processing_script import process_performance_reports


from data_ingestion.quarterly_performance_reports_data_ingestion import ingest_performance_reports

# Define the Airflow DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=0),
}



performance_reports_dag = DAG(
    'ETL_quarterly_performance_reports',
    default_args=default_args,
    description='A DAG to process performance reports and upload them to S3',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    tags=[ 'quarterly_performance_reports'],
)









# Quaerterly performance reports tasks 

process_performance_reports_task = PythonOperator(
    task_id='process_performance_reports_task',
    python_callable=process_performance_reports,
    dag=performance_reports_dag,
)

ingest_performance_reports_task = PythonOperator(
    task_id='ingest_performance_reports_task',
    python_callable=ingest_performance_reports,
    dag=performance_reports_dag,
)


process_performance_reports_task >> ingest_performance_reports_task
