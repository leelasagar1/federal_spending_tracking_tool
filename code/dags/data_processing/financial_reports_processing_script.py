import time
import us
import pandas as pd
import os
from trp import Document
import re

from helpers.aws_utils import get_client,save_to_s3,move_file_in_s3
from helpers.date_extractor import get_report_date

def get_response(bucket_name, file_name):
    client = get_client('textract')
    response = client.start_document_analysis(
        DocumentLocation={
            'S3Object': {
                'Bucket': bucket_name,
                'Name': file_name
            }
        },
        FeatureTypes=['TABLES']
    )
    job_id = response['JobId']

    # Wait for job completion
    while True:
        response = client.get_document_analysis(JobId=job_id)
        status = response['JobStatus']
        if status in ['SUCCEEDED', 'FAILED']:
            break
        time.sleep(5)

    if status == 'FAILED':
        raise Exception("Document analysis failed")

    # Collect all pages of the response
    all_blocks = response['Blocks']
    next_token = response.get('NextToken')

    while next_token:
        response = client.get_document_analysis(JobId=job_id, NextToken=next_token)
        all_blocks.extend(response['Blocks'])
        next_token = response.get('NextToken')

    response['Blocks'] = all_blocks
    return response


def convert_response_to_dataframe(response):
    doc = Document(response)
    result_df = pd.DataFrame()

    columns = []
    for page in doc.pages:
        for table in page.tables:
            try:
                table_data = [[cell.text if cell.text else "" for cell in row.cells] for row in table.rows]
                df = pd.DataFrame(table_data)
                if 'grantee' in df.iloc[0,0].strip().lower():
                    if len(columns) ==0:
                        columns = df.iloc[0].str.strip()
                    df = df[1:]

                df.columns = columns
                result_df = pd.concat([result_df, df], ignore_index=True)
            except Exception as e:
                print('table not processed because of error:',e)

    result_df.columns = result_df.columns.str.strip()
    return result_df

def process_raw_data(data,old_data_files = False):
    # Fill missing 'Grantee' values with empty strings for processing
    
    data = data.replace('', pd.NA)
    if old_data_files:
        data.columns = ['Grantee','Grant','Grant Award','Balance','Last Month Spending','Spending Status']
        data = data.drop(columns=['Last Month Spending'],axis=1)
    else:
        data.columns = [ "Grantee","Grant ID", "Grant Award", "Balance", "Disaster Year",
            "Grant Age in Months", "Expected Spending %", "Drawn %",
            "Spending Status", "Amount Behind Pace" ]
        
    fixed_grantee = data['Grantee'].fillna('')

    # Define continuation values and merge them with previous rows
    continuation_values = [
        ',', 'Rouge, LA', 'AL', 'Samoa', 'TX', 'DOH', '(NYS)', 'OCRA', 'County, PA', 'PA', 'FL',
        'Development Corporation (NYS)', 'LA', 'County, HI', 'County, AL', 'Charles, LA',
        'County, PA', 'Davidson, TN', 'Orleans, LA', 'City, NY', 'Carolina- NCORR', 'Islands',
        'Carolina', 'County, SC', 'MA', 'MI', 'NY', 'SC'
    ]

    # Merge continuation rows by iterating a few times
    for _ in range(3):
        for i in range(1, len(fixed_grantee)):
            if fixed_grantee[i].strip() in continuation_values:
                fixed_grantee[i - 1] = f"{fixed_grantee[i - 1].strip()} {fixed_grantee[i].strip()}"
                fixed_grantee[i] = ''  # Clear the current row after merging

    # Forward fill the cleaned 'Grantee' column
    data['Grantee'] = fixed_grantee.replace('', pd.NA).fillna(method='ffill')

    # Merge rows where Grantee is not empty but all other columns are empty
    for i in range(1, len(data)):
        if pd.isna(data.iloc[i, 1:]).all() and pd.notna(data.iloc[i, 0]):
            data.iloc[i - 1, 0] = f"{data.iloc[i - 1, 0]} {data.iloc[i, 0]}"
            data.iloc[i, 0] = None  # Mark for deletion

    # Drop rows where Grantee was merged and reset index
    data = data.dropna(subset=['Grantee']).reset_index(drop=True)
    
    data.rename(columns={
        'Grantee': 'State',
        'Grant': 'Grant ID'
    }, inplace=True)
    return data

def clean_currency_column(value):

    # Replace parentheses with negative sign
    value = value.replace('(', '-').replace(')', '')
    # Remove dollar signs and other non-numeric characters except for negative sign
    value = re.sub(r'[^-\d]', '', value)
    # Handle cases like "$ -" or "$" by returning 0
    if value == '' or value == '-':
        return 0
    # Convert to numeric value
    return pd.to_numeric(value)

def clean_percentage_column(column):
    return column.str.replace('%', '').str.replace(',', '.').str.strip().apply(lambda x: float(x) if not pd.isna(x) else x)

def prepare_for_insert(df, state_code):
    # Defining expected columns and default values for missing columns

    selected_columns = [
            "Grant ID", "State", "Grant Award", "Balance", "Disaster Year",
            "Grant Age in Months", "Expected Spending %", "Drawn %",
            "Spending Status", "Amount Behind Pace"
        ]
    # Add missing columns with default values
    for column in selected_columns:
        if column not in df.columns:
            df[column] = None

    # Select only the expected columns (in case there are extra columns in the dataframe)
    # selected_columns = list(default_columns.keys())
    grant_financial_reports_df = df[selected_columns].copy()

    # Rename columns to match the desired format
    grant_financial_reports_df.rename(columns={
        "Grant ID": "grant_id",
        "State": "state_code",
        "Grant Award": "grant_award",
        "Balance": "balance",
        "Disaster Year": "disaster_year",
        "Grant Age in Months": "grant_age_in_months",
        "Expected Spending %": "expected_spending_percentage",
        "Drawn %": "drawn_percentage",
        "Spending Status": "spending_status",
        "Amount Behind Pace": "amount_behind_pace"
    }, inplace=True)

    # Clean and convert data types
    grant_financial_reports_df['grant_award'] = grant_financial_reports_df['grant_award'].apply(clean_currency_column)
    grant_financial_reports_df['balance'] = grant_financial_reports_df['balance'].apply(clean_currency_column)
    grant_financial_reports_df['amount_behind_pace'] = grant_financial_reports_df['amount_behind_pace'].apply(clean_currency_column)

    grant_financial_reports_df['expected_spending_percentage'] = clean_percentage_column(grant_financial_reports_df['expected_spending_percentage'])
    grant_financial_reports_df['drawn_percentage'] = clean_percentage_column(grant_financial_reports_df['drawn_percentage'])

    # Convert state names to state abbreviations
    grant_financial_reports_df.loc[:, 'state_code'] = grant_financial_reports_df['state_code'].apply(
        lambda x: us.states.lookup(x.strip()).abbr if pd.notna(x) and us.states.lookup(x.strip()) else x
    )

    # Filter by the given state code
    result_df = grant_financial_reports_df[grant_financial_reports_df['state_code'] == state_code]

    return result_df



    
def process_financial_reports():
    # Configuration
    source_bucket_name = 'project-raw-data-files'
    source_file_folder = 'financial_reports'
    destination_bucket_name = 'project-processed-data'  # The bucket where processed files will be saved
    destination_folder = 'financial_reports'  # Destination folder inside the destination bucket
    state_code = 'PR'
    processed_folder = f'archived/{destination_folder}'

    s3 = get_client('s3')
    response = s3.list_objects_v2(Bucket=source_bucket_name, Prefix=source_file_folder)
    
    if 'Contents'  in response:
        print("No files found in the specified folder.")
        return

    for obj in response['Contents'][-2:]:
        source_file_path = obj['Key']
        source_file_name = os.path.basename(source_file_path)
        if source_file_name.endswith('.pdf'):
            print(f"Processing file: {source_file_name}")
            output_file_key = os.path.join(destination_folder, f"processed_{source_file_name.replace('.pdf', '.csv')}")
            break
            # get year from file name
            old_data_files = False

            report_date = get_report_date(source_bucket_name,source_file_path)
            year = int(report_date.split('-')[0])
            
            if year!=None and year <2022:
                old_data_files = True

            # Process the document
            response = get_response(source_bucket_name, source_file_path)
            result_df = convert_response_to_dataframe(response)
            processed_result_df = process_raw_data(result_df,old_data_files)
            final_df = prepare_for_insert(processed_result_df, state_code)
            final_df['report_date'] = report_date

            # Save the result to S3
            save_to_s3(final_df, destination_bucket_name, output_file_key)

            # Move the processed file to another folder in the source bucket
            move_file_in_s3(source_bucket_name, source_file_path, source_bucket_name, f"{processed_folder}/{source_file_name}")

            print(f"Processed file saved to S3: s3://{destination_bucket_name}/{output_file_key}")
