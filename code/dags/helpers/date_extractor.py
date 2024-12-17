from io import BytesIO
from datetime import datetime
import re
import fitz 
import pandas as pd
from helpers.aws_utils import get_client
def extract_dates_from_pdf(bucket_name, file_key):
    # Create an S3 client
    s3 = get_client('s3')

    # Download the file from S3 into a BytesIO object
    file_obj = BytesIO()
    s3.download_fileobj(bucket_name, file_key, file_obj)
    
    # Open the PDF from the BytesIO object
    file_obj.seek(0)  # Ensure the file pointer is at the beginning
    with fitz.open("pdf", file_obj) as pdf:
        # Iterate over the first page of the PDF
        page = pdf.load_page(0)
        text = page.get_text()

        # Find all dates in the format 'Month Day, Year' or 'YYYY-MM-DD'
        found_date = re.findall(r'\b(?:January|February|March|April|May|June|July|August|September|October|November|December) \d{1,2}, \d{4}\b', text)
        found_date += re.findall(r'\b\d{4}-\d{2}-\d{2}\b', text)

    return found_date

def convert_dates(date):

    formatted_date = datetime.strptime(date, '%B %d, %Y').strftime('%Y-%m-%d')

    return formatted_date

def extract_date_from_filename(filename):
    # Extract the year and month from the filename using regex
    match = re.search(r'(\d{4})-(\d{1,2})', filename)
    if match:
        year = match.group(1)
        month = match.group(2).zfill(2)  # Add leading zero to month if needed
        date_str = f"{year}-{month}-01"  # Create a date string in the format YYYY-MM-DD

        try:
            # Convert to a proper date format
            formatted_date = datetime.strptime(date_str, '%Y-%m-%d').strftime('%Y-%m-%d')
            return formatted_date
        except ValueError:
            # If the date format is not valid, return None
            return None
    return None


def get_report_date(bucket_name, file_key):


    extracted_date = extract_dates_from_pdf(bucket_name, file_key)
    if len(extracted_date) >0:

        report_date = convert_dates(extracted_date[0])

    else:
        report_date =  extract_date_from_filename(file_key)

    return report_date

def transform_date_column(date_str):
    # Dictionary to replace abbreviated months with full names for parsing
    month_abbreviation_map = {
        'Jan.': 'Jan', 'Feb.': 'Feb', 'Mar.': 'Mar', 'Apr.': 'Apr', 'May.': 'May',
        'Jun.': 'Jun', 'Jul.': 'Jul', 'Aug.': 'Aug', 'Sept.': 'Sep', 'Oct.': 'Oct',
        'Nov.': 'Nov', 'Dec.': 'Dec'
    }
    
    if pd.isna(date_str) or date_str is None:
        return None
    
    for abbr, full in month_abbreviation_map.items():
        date_str = date_str.replace(abbr, full)
    
    # Convert using pd.to_datetime with errors set to 'coerce' to handle errors
    standard_date = pd.to_datetime(date_str, errors='coerce')
    if pd.isna(standard_date):
        return None
    else:
        return standard_date.strftime('%Y-%m-%d')
