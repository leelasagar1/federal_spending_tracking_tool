**README for ETL Pipelines Using Airflow**

# ETL Pipelines Project

This project involves building and managing ETL (Extract, Transform, Load) pipelines for a client, using Apache Airflow as the orchestration tool. The pipelines are designed to extract data from five different data sources, process the data, and store it in an AWS RDS PostgreSQL database. Intermediate storage is handled using AWS S3, and PDF extraction is done using AWS Textract.

## Project Overview
The project contains four distinct ETL pipelines to manage data from different sources:

1. **Pipeline 1: Financial Reports Data Extraction (ETL_financial_reports)**
   - **Task 1: Data Extraction** (`web_scraper.py`)
     - Scrape the financial PDF files from the designated web page and save the scraped PDF files in the AWS S3 `raw` bucket.
   - **Task 2: Data Processing** (`financial_reports_processing_script.py`)
     - Use AWS Textract to extract relevant information from the financial PDF files and save the processed output in the S3 `processed` bucket.
   - **Task 3: Data Loading** (`financial_reports_data_ingestion.py`)
     - Fetch the processed CSV files from the `processed` bucket and ingest the data into AWS RDS PostgreSQL tables.

**Pipeline 2: History Reports Data Extraction (ETL_history_reports)**
   - **Task 1: Data Extraction** (`web_scraper.py`)
     - Scrape the history PDF files from the designated web page and save the scraped PDF files in the AWS S3 `raw` bucket.
   - **Task 2: Data Processing** (`history_reports_processing_script.py`)
     - Use AWS Textract to extract relevant information from the history PDF files and save the processed output in the S3 `processed` bucket.
   - **Task 3: Data Loading** (`history_reports_data_ingestion.py`)
     - Fetch the processed CSV files from the `processed` bucket and ingest the data into AWS RDS PostgreSQL tables.

2. **Pipeline 3: USA Spending Data Extraction (ETL_usa_spending)**
   - **Task 1: Data Extraction** (`usa_spending_extraction_script.py`)
     - Use Python's `requests` library to make GET requests to the API and store the retrieved data in JSON format in the AWS S3 `raw` bucket.
   - **Task 2: Data Processing** (`usa_spending_processing_script.py`)
     - Transform the JSON data as needed (e.g., normalizing nested fields) and save the processed data in the S3 `processed` bucket.
   - **Task 3: Data Loading** (`usa_spending_data_ingestion.py`)
     - Load the processed data into PostgreSQL tables.

3. **Pipeline 4: Quarterly Performance Reports (ETL_quarterly_performance_reports)**
   - **Task 1: Manual Task**
     - Manually download the PDF files from the provided sources and upload specific pages to the S3 `raw` bucket.
   - **Task 2: Data Extraction and Processing** (`quarterly_performance_reports_processing_script.py`)
     - Extract and process the data from the uploaded PDF pages using AWS Textract and save the processed output in the `processed` bucket.
   - **Task 3: Data Loading** (`quarterly_performance_reports_data_ingestion.py`)
     - Load the processed data into PostgreSQL tables.
   - Extract data from multiple sources including USA Spending reports and quarterly performance reports.
   - Use Apache Airflow to manage separate ETL workflows for financial, history, and performance reports.
   - Store raw data in S3 (`raw` bucket), process the data, and save the processed output in the `processed` bucket.
   - Load the processed data into AWS RDS PostgreSQL tables.

## Technologies Used
- **Apache Airflow**: Orchestration of ETL workflows.
- **AWS Textract**: Extract data from PDF files.
- **AWS S3**: Intermediate storage for raw and processed data.
- **AWS RDS PostgreSQL**: Data warehouse for storing final processed data.

## Repository Structure
```
.
├── dags/
│   ├── financial_reports_dag.py                # DAG for financial reports ETL
│   ├── history_reports_dag.py                  # DAG for history reports ETL
│   ├── quarterly_performance_reports_dag.py    # DAG for quarterly performance reports ETL
│   └── usa_spending_dag.py                     # DAG for USA spending reports ETL
├── README.md
├── requirements.txt                            # Python dependencies
├── config/
│   └── config.json                             # Configuration details (e.g., S3 buckets, API endpoints)
└── scripts/
    ├── data_extraction/
    │   ├── web_scraper.py                      # Script to scrape financial and history reports
    │   ├── usa_spending_extraction_script.py   # Script to extract USA spending data
    ├── data_ingestion/
    │   ├── financial_reports_data_ingestion.py # Script to ingest financial reports into PostgreSQL
    │   ├── history_reports_data_ingestion.py   # Script to ingest history reports into PostgreSQL
    │   ├── quarterly_performance_reports_data_ingestion.py # Script to ingest quarterly performance reports
    │   └── usa_spending_data_ingestion.py      # Script to ingest USA spending data
    ├── data_processing/
    │   ├── financial_reports_processing_script.py  # Script to process financial reports
    │   ├── history_reports_processing_script.py    # Script to process history reports
    │   ├── quarterly_performance_reports_processing_script.py # Script to process performance reports
    │   └── usa_spending_processing_script.py      # Script to process USA spending data
    └── helpers/
        ├── aws_utils.py                        # Utility functions for AWS operations
        ├── db_utils.py                         # Utility functions for database operations
        └── utils.py                            # General utility functions
```

## Getting Started

### Prerequisites
- Python 3.8+
- Docker & Docker Compose (for running Airflow)
- AWS CLI configured with the appropriate credentials

### Installation
1. Clone the repository:
   ```bash
   git clone https://github.com/username/etl-pipelines.git
   cd etl-pipelines
   ```
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Set up Apache Airflow using Docker Compose:
   ```bash
   docker-compose up
   ```

### Configuration
- Update the `config/config.json` file with relevant settings like S3 bucket names, API endpoints, and AWS credentials.

### Running the Pipelines
- To trigger the pipelines manually, go to the Airflow web UI (usually accessible at `http://localhost:8080`) and enable the DAGs as needed.

## Data Flow
1. **Extraction**: Raw data is fetched from web pages, APIs, or manually uploaded PDFs.
2. **Transformation**: Data is extracted from PDF files using AWS Textract and processed for consistency.
3. **Loading**: The transformed data is saved in AWS RDS PostgreSQL tables for downstream use.

## Key Considerations
- **Manual Steps**: For sources that require manual intervention, manual work is needed to upload specific pages to the S3 bucket.
- **Data Quality**: Ensure data quality checks are performed during processing to maintain consistency.

## Future Enhancements
- Automate the manual step in Pipeline 4 using an OCR script to reduce human intervention.
- Add monitoring and alerting for pipeline failures.

---

**Procedures Document for ETL Pipelines Project**

# Procedures for ETL Pipelines Project

## Overview
This document outlines the detailed procedures followed in the ETL pipeline project developed for the client. The project focuses on extracting data from multiple sources, transforming it, and loading it into a PostgreSQL database. The workflows are managed using Apache Airflow, leveraging AWS services for storage and processing.

## Step-by-Step Procedures

### Pipeline 1 & 2: Webpage PDF Data Extraction (ETL_financial_reports, ETL_history_reports)
1. **Step 1: Data Extraction**
   - Scrape the PDF files from the designated web pages using Selenium or similar tools.
   - Save the scraped PDF files in the AWS S3 `raw` bucket.
2. **Step 2: Data Processing**
   - Use AWS Textract to extract relevant information from the PDF files.
   - Process the extracted data into a structured format (CSV).
   - Save the processed data in the S3 `processed` bucket.
3. **Step 3: Data Loading**
   - Fetch the processed CSV files from the `processed` bucket.
   - Ingest the data into AWS RDS PostgreSQL tables.

### Pipeline 3: USA Spending Data Extraction (ETL_usa_spending)
1. **Step 1: Data Extraction**
   - Use Python's `requests` library to make GET requests to the API.
   - Store the retrieved data in JSON format in the AWS S3 `raw` bucket.
2. **Step 2: Data Processing**
   - Transform the JSON data as needed (e.g., normalizing nested fields).
   - Save the processed data in the S3 `processed` bucket.
3. **Step 3: Data Loading**
   - Load the processed data into PostgreSQL tables.

### Pipeline 4: Quarterly Performance Reports (ETL_quarterly_performance_reports)
1. **Manual Task**
   - Manually download the PDF files from the provided sources and upload specific pages to the S3 `raw` bucket.
2. **Step 1: Data Extraction and Processing**
   - Extract and process the data from the uploaded PDF pages using AWS Textract.
   - Save the processed output in the `processed` bucket.
3. **Step 2: Data Loading**
   - Load the processed data into PostgreSQL tables.1. **Step 1: Data Extraction**
   - Extract USA Spending reports and quarterly performance reports using relevant extraction scripts.
   - Store the raw data in the AWS S3 `raw` bucket.
2. **Step 2: Data Processing**
   - Process the extracted data to convert it into a structured format.
   - Save the processed data in the S3 `processed` bucket.
3. **Step 3: Data Loading**
   - Ingest the processed data into AWS RDS PostgreSQL tables.

## Scheduling and Monitoring
- All ETL pipelines are scheduled to run daily to ensure up-to-date data is available.
- Monitoring is implemented using Airflow's built-in logging and email alerting for task failures.

## Error Handling
- **Data Extraction Failures**: Retry logic is implemented for web scraping and API requests to handle intermittent network issues.
- **Data Transformation Errors**: Validation checks are applied to processed data, and any discrepancies are logged for further analysis.

## Security Considerations
- AWS credentials are stored securely using Airflow's connections feature.
- Data in S3 is encrypted, and access is restricted through IAM policies.

## Manual Intervention Points
- For the quarterly performance reports pipeline, manual intervention is needed to download and save specific pages from PDF files to the S3 bucket (`raw`).

## Lessons Learned
- Automation of manual tasks can significantly enhance pipeline efficiency.
- Incorporating data quality checks early in the pipeline prevents downstream issues.

---

Please let me know if you need any additional information or if there are specific details you would like me to include in these documents.

