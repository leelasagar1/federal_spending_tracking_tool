import sys
import os


from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

from data_extraction.web_scraper import (scrape_and_upload_financial_reports, 
                                 scrape_and_upload_history_reports)
from data_extraction.usa_spending_extraction_script import extract_usa_spending_data

from data_processing.financial_reports_processing_script import process_financial_reports
from data_processing.quarterly_performance_reports_processing_script import process_performance_reports
from data_processing.history_reports_processing_script import process_history_reports
from data_processing.usa_spending_processing_script import process_usa_spending_data

from data_ingestion.quarterly_performance_reports_data_ingestion import ingest_performance_reports
from data_ingestion.financial_reports_data_ingestion import ingest_financial_reports
from data_ingestion.usa_spending_data_ingestion import ingest_usa_spending_data
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

financial_reports_dag = DAG(
    'ETL_financial_reports',
    default_args=default_args,
    description='A DAG to scrape and process financial reports and upload them to S3',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    tags=['webscraping', 's3', 'financial_reports'],
)

history_reports_dag = DAG(
    'ETL_history_reports',
    default_args=default_args,
    description='A DAG to scrape history reports and upload them to S3',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    tags=['webscraping', 's3', 'hisotry_reports'],
)

performance_reports_dag = DAG(
    'ETL_quarterly_performance_reports',
    default_args=default_args,
    description='A DAG to process performance reports and upload them to S3',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    tags=[ 'quarterly_performance_reports'],
)

usa_spending_dag = DAG(
    'ETL_usa_spending',
    default_args=default_args,
    description='A DAG to extract and process USA performance reports and upload them to S3',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    tags=[ 'USASpending'],
)



# Financial reports tasks

fetch_financial_reports_task = PythonOperator(
    task_id='fetch_financial_reports_task',
    python_callable=scrape_and_upload_financial_reports,
    dag=financial_reports_dag,
)

process_financial_reports_task = PythonOperator(
    task_id='process_financial_reports_task',
    python_callable=process_financial_reports,
    dag=financial_reports_dag,
)

ingest_financial_reports_task = PythonOperator(
    task_id='ingest_financial_reports_task',
    python_callable=ingest_financial_reports,
    dag=financial_reports_dag,
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

fetch_financial_reports_task >> process_financial_reports_task >> ingest_financial_reports_task
fetch_history_reports_task  >> process_history_reports_task >> ingest_history_reports_task
process_performance_reports_task >> ingest_performance_reports_task
extract_usa_spending_data_task >> process_usa_spending_data_task >> ingest_usa_spending_data_task