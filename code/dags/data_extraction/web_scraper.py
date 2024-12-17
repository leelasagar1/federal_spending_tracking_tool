import requests
import boto3
from bs4 import BeautifulSoup
from helpers import get_client
# from plugins.s3_utils import get_client



# Configure S3 client
s3_client = get_client('s3')

# Define the function to fetch and upload reports
def fetch_cdbg_dr_reports(base_url, target_file_name, bucket_name,folder_name):
    response = requests.get(base_url)
    response.raise_for_status()

    soup = BeautifulSoup(response.content, 'html.parser')
    links = soup.find_all('a', href=True)

    for link in links:
        href = link['href']
        if target_file_name in link.text and href.endswith('.pdf'):
            report_url = href if href.startswith('http') else 'https://www.hud.gov/' + href
            file_name = href.split('/')[-1]
            raw_data_key = f"{folder_name}/{file_name}"
            archived_data_key = f"archived/{folder_name}/{file_name}"

            raw_folder_files = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=raw_data_key)
            archived_folder_files = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=archived_data_key)
            if 'Contents' in raw_folder_files or 'Contents' in archived_folder_files:
                print(f"File {file_name} already exists in S3. Skipping upload.")
                continue

            print(f"Downloading: {file_name}")
            report_response = requests.get(report_url)
            report_response.raise_for_status()

            s3_client.put_object(Bucket=bucket_name, Key=raw_data_key, Body=report_response.content)
            print(f"Uploaded: {raw_data_key} to bucket {bucket_name}")


def scrape_and_upload_financial_reports():
    REPORTS_URL = 'https://www.hud.gov/program_offices/comm_planning/cdbg-dr/reports'
    target_file_name = 'CDBG-DR Grants Financial Report'
    bucket_name = 'project-raw-data-files'
    folder_name='financial_reports'
    fetch_cdbg_dr_reports(REPORTS_URL, target_file_name, bucket_name,folder_name)

def scrape_and_upload_history_reports():
    REPORTS_URL = 'https://www.hud.gov/program_offices/comm_planning/cdbg-dr/reports'
    target_file_name = 'CDBG-DR Grant History Report'
    bucket_name = 'project-raw-data-files'
    folder_name='history_reports'
    fetch_cdbg_dr_reports(REPORTS_URL, target_file_name, bucket_name,folder_name)
