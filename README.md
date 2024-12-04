# ETL Pipelines Using Apache Airflow 🚀

Effortlessly manage complex ETL workflows with this project designed to extract, transform, and load data from multiple sources into an AWS RDS PostgreSQL database. Leveraging the power of **Apache Airflow** for orchestration, this system ensures smooth data flow from raw data ingestion to final storage. AWS services like S3 and Textract bring scalability and automation to the pipeline, making it reliable and efficient.

---

## 🌟 **Project Highlights**
- **Four Dynamic ETL Pipelines:** Tailored for financial reports, historical data, USA spending data, and quarterly performance metrics.
- **AWS-Powered Workflows:** Utilize S3 for storage, Textract for PDF data extraction, and RDS PostgreSQL for data warehousing.
- **Orchestrated with Apache Airflow:** Simplify scheduling, monitoring, and error handling across all pipelines.
- **Containerized with Docker Compose:** Seamless deployment and management of the ETL pipelines.
- **Comprehensive Data Processing:** Transform raw data into a structured format ready for analysis.

---

## 🗂️ **Project Overview**

### **Pipeline 1: Financial Reports Extraction**
- **Extract:** Scrape financial PDFs via `web_scraper.py` and store in S3 (`raw` bucket).
- **Transform:** Use AWS Textract (`financial_reports_processing_script.py`) to process PDFs into structured data stored in the `processed` bucket.
- **Load:** Save the processed data into PostgreSQL tables using `financial_reports_data_ingestion.py`.

### **Pipeline 2: History Reports Extraction**
- **Extract:** Scrape historical data PDFs via `web_scraper.py` into S3 (`raw` bucket).
- **Transform:** Process the PDFs with AWS Textract (`history_reports_processing_script.py`) and save results in the `processed` bucket.
- **Load:** Store the processed data in PostgreSQL using `history_reports_data_ingestion.py`.

### **Pipeline 3: USA Spending Data Extraction**
- **Extract:** Fetch data using APIs (`usa_spending_extraction_script.py`) and store it in S3 (`raw` bucket).
- **Transform:** Normalize and structure the data (`usa_spending_processing_script.py`) and save to the `processed` bucket.
- **Load:** Import the cleaned data into PostgreSQL using `usa_spending_data_ingestion.py`.

### **Pipeline 4: Quarterly Performance Reports**
- **Extract:** Manual upload of specific pages from quarterly PDF files to S3 (`raw` bucket).
- **Transform:** Extract and process data using Textract (`quarterly_performance_reports_processing_script.py`) and save in `processed` bucket.
- **Load:** Ingest the processed data into PostgreSQL with `quarterly_performance_reports_data_ingestion.py`.

---

## ⚙️ **Technologies Used**
- **Apache Airflow**: Orchestrates ETL workflows with easy-to-manage DAGs.
- **AWS S3**: Seamlessly handles raw and processed data storage.
- **AWS Textract**: Extracts structured data from complex PDF documents.
- **PostgreSQL**: Reliable storage for final processed data.
- **Docker Compose**: Simplifies deployment and environment setup.
- **Python**: Powers the scripts for data extraction, transformation, and ingestion.

---

## 🚀 Getting Started

### Prerequisites
- Python 3.8+
- Docker & Docker Compose (for running Airflow)
- AWS CLI configured with the appropriate credentials

### Installation
1. Clone the repository:
   ```bash
   git clone https://github.com/leelasagar1/federal_spending_tracking_tool.git
   cd federal_spending_tracking_tool
   ```
2. Configure AWS credentials: Update the config/aws_config.json file with your AWS details:
   ```bash
      {
         "aws_access_key_id": "ADD AWS ACCESS KEY",
         "aws_secret_access_key": "ADD AWS SECRET ACCESS KEY",
         "region_name": "ADD REGION"
      }
   ```
3. Set up Apache Airflow using Docker Compose:
   ```bash
   docker-compose up
   ```


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

