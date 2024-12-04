import boto3
import time
import datetime
from trp import Document
import pandas as pd
import os
import re
from io import StringIO
from helpers.aws_utils import (get_client,
                               move_file_in_s3,
                               save_to_s3
                                )

from helpers.db_utils import get_max_value
# get main table


# Start Textract Document Analysis and Get Complete Response
def get_response(client,bucket_name, file_name,types=['TABLES']):
    
    response = client.start_document_analysis(
        DocumentLocation={
            'S3Object': {
                'Bucket': bucket_name,
                'Name': file_name
            }
        },
        FeatureTypes= types
    )
    job_id = response['JobId']

    # Polling for job completion
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
    next_token = response.get('NextToken', None)

    while next_token:
        response = client.get_document_analysis(JobId=job_id, NextToken=next_token)
        all_blocks.extend(response['Blocks'])
        next_token = response.get('NextToken', None)

    response['Blocks'] = all_blocks
    return response

def convert_response_to_dataframe(response):
    doc = Document(response)
    result_df = pd.DataFrame()

    for page in doc.pages:
        for table in page.tables:
            table_data = [[cell.text if cell.text else "" for cell in row.cells] for row in table.rows]
            df = pd.DataFrame(table_data)
            df.columns = df.iloc[0]
            df = df[1:]
            result_df = pd.concat([result_df, df], ignore_index=True)
    
    result_df.columns = result_df.columns.str.strip()
    return result_df

# Extract Table from Specific Page and Index
def extract_table(doc, page_idx, table_idx, columns=None, skip_rows=0):
    try:
        page = doc.pages[page_idx]
        table = page.tables[table_idx]
        table_data = [
            [cell.text if cell.text else "" for cell in row.cells]
            for row in table.rows
        ]
        df = pd.DataFrame(table_data)
        if columns:
            df.columns = columns
        else:
            df.columns = df.iloc[0]
        if skip_rows:
            df = df[skip_rows:]
        return df
    except (IndexError, AttributeError):
        return pd.DataFrame()

# Extract Multiple Tables from a List of Page and Table Indices
def extract_multiple_tables(doc, page_table_indices, columns):
    result_df = pd.DataFrame()
    for page_idx, table_indices in page_table_indices.items():
        for table_idx in table_indices:
            df = extract_table(doc, page_idx, table_idx, columns)
            result_df = pd.concat([result_df, df], ignore_index=True)
    return result_df.replace('', pd.NA).dropna()



def is_grant_id(first_field):
    """
    Checks if the first field matches the Grant ID pattern.
    The Grant ID pattern is assumed to be 'B-XX-XX-XX-XXXX', where X can be digits or uppercase letters.
    """
    grant_id_pattern = re.compile(r'^B-\d{2}-[A-Z]{2}-\d{2}-\d{4}$')
    return bool(grant_id_pattern.match(first_field.strip()))

def is_description(first_field):
    """
    Checks if the first field is a description (i.e., not a Grant ID).
    A description does not match the Grant ID pattern.
    """
    return not is_grant_id(first_field)

def process_line(line, current_description, description_id_map, ID_counter, table1, table2):
    """
    Processes a single line from the input DataFrame.
    Updates the current description, description ID map, and appends to table1 or table2 as appropriate.
    """
    first_field = line[0].strip()
    to_date = line[2]
    amount = line[1]

    if is_description(first_field):
        current_description = first_field
        if current_description not in description_id_map:
            description_id_map[current_description] = ID_counter
            ID_counter += 1
        # Add to table2
        table2.append({
            'id': description_id_map[current_description],
            'description': current_description,
            'to_date_amount': to_date,
            'time_period_amount': amount
        })
    elif is_grant_id(first_field):
        grant_id = first_field
        description_id = description_id_map.get(current_description)
        if description_id is not None:
            # Add to table1
            table1.append({
                'description_id': description_id,
                'grant_id': grant_id,
                'to_date_amount': to_date,
                'time_period_amount': amount
            })
    else:
        # Handle any unexpected cases
        pass

    return current_description, ID_counter

def process_dataframe(df,ID_counter=1):
    """
    Processes the entire DataFrame and returns two DataFrames: table1 and table2.
    """
    current_description = None
    description_id_map = {}
    table1 = []
    table2 = []

    for index, line in df.iterrows():
        current_description, ID_counter = process_line(
            line, current_description, description_id_map, ID_counter, table1, table2
        )

    table1_df = pd.DataFrame(table1)
    table2_df = pd.DataFrame(table2)

    return table1_df, table2_df





def define_queries():
    """
    Define and return the list of queries to be used in Textract analysis.
    """
    return [
        {"Text": "What is the Grantee Activity Number?", "Alias": "Grantee_Activity_Number"},
        {"Text": "What is the Activity Title?", "Alias": "Activity_Title"},
        {"Text": "What is the Activity Type?", "Alias": "Activity_Type"},
        {"Text": "What is the Activity Status?", "Alias": "Activity_Status"},
        {"Text": "What is the Project Number?", "Alias": "Project_Number"},
        {"Text": "What is the Project Title?", "Alias": "Project_Title"},
        {"Text": "What is the Projected Start Date?", "Alias": "Projected_Start_Date"},
        {"Text": "What is the Projected End Date?", "Alias": "Projected_End_Date"},
        {"Text": "What is the Benefit Type?", "Alias": "Benefit_Type"},
        {"Text": "What is the Completed Activity Actual End Date?", "Alias": "Completed_Activity_Actual_End_Date"},
        {"Text": "What is the National Objective?", "Alias": "National_Objective"},
        {"Text": "What is the Responsible Organization?", "Alias": "Responsible_Organization"},
    ]

def analyze_document(textract_client, s3_bucket, document_name, queries):
    """
    Analyze the document using Amazon Textract and return the response.
    """
    return textract_client.analyze_document(
        Document={'S3Object': {'Bucket': s3_bucket, 'Name': document_name}},
        FeatureTypes=["QUERIES"],
        QueriesConfig={"Queries": queries}
    )

def extract_query_results(response):
    """
    Extract query results from the Textract response and return them as a dictionary.
    """
    query_results = {}
    blocks = response.get('Blocks', [])
    for block in blocks:
        if block['BlockType'] == 'QUERY':
            alias = block['Query'].get('Alias', 'No_Alias')
            # Check if 'Relationships' key exists and is not empty
            if 'Relationships' in block and block['Relationships']:
                answer_block_id = block['Relationships'][0]['Ids'][0]
                answer_block = next((b for b in blocks if b['Id'] == answer_block_id), None)
                if answer_block and answer_block['BlockType'] == 'QUERY_RESULT':
                    answer_text = answer_block.get('Text', 'No answer found')
                    query_results[alias] = answer_text
            else:
                # Handle cases where no answer is found
                query_results[alias] = 'No answer found'
    return query_results

def results_to_dataframe(results):
    """
    Convert the extracted results dictionary to a pandas DataFrame.
    """
    return pd.DataFrame([results])


    # Initialize Textract client

def extract_through_queries(textract_client,s3_bucket,document_name):
    # Define queries
    queries = define_queries()

    # Analyze document
    response = analyze_document(textract_client, s3_bucket, document_name, queries)

    # Extract query results
    extracted_info = extract_query_results(response)

    # Convert results to DataFrame
    df = results_to_dataframe(extracted_info)

    return df

def process_performance_reports_dataframe(performance_reports_df):
    performance_id = 128
    performance_reports_df['performance_id'] = list(range(performance_id,len(performance_reports_df)+performance_id))
    performance_reports_df = performance_reports_df[['performance_id','activity_id','description_id', 'grant_id', 'to_date_amount', 'time_period_amount']]
    performance_reports_df['time_period_amount'] = performance_reports_df['time_period_amount'].replace('[\$, ]', '', regex=True).astype(float)
    performance_reports_df['to_date_amount'] = performance_reports_df['to_date_amount'].replace('[\$, ]', '', regex=True).astype(float)
    return performance_reports_df

def process_activity_dataframe(final_activity_df):
    final_activity_df = final_activity_df[['activity_id','Grantee_Activity_Number', 'Activity_Type','Activity_Status','Project_Number', 'Project_Title',
       'Projected_Start_Date', 'Projected_End_Date','National_Objective','Responsible_Organization', 'Benefit_Type','Activity_Title',
       'Completed_Activity_Actual_End_Date'
       ]]
    final_activity_df['Completed_Activity_Actual_End_Date'] = final_activity_df['Completed_Activity_Actual_End_Date'].apply(lambda x: pd.NA if x=='No answer found' else x )
    return final_activity_df

def process_amount_description_dataframe(amount_description_df):

    amount_description_df['time_period'] = 'Apr 1 thru Jun 30, 2024'
    amount_description_df = amount_description_df[['id', 'description', 'time_period_amount', 'time_period', 'to_date_amount']]
    amount_description_df['time_period_amount'] = amount_description_df['time_period_amount'].replace('[\$, ]', '', regex=True).astype(float)
    amount_description_df['to_date_amount'] = amount_description_df['to_date_amount'].replace('[\$, ]', '', regex=True).astype(float)
    return amount_description_df




def process_performance_reports():
    client = get_client('textract')


    source_bucket_name = 'project-raw-data-files'
    source_file_folder = 'quarterly_performance_reports'
    destination_bucket_name = 'project-processed-data'  # The bucket where processed files will be saved
    destination_folder = 'quarterly_performance_reports'  # Destination folder inside the destination bucket
    processed_folder = f'archived/{destination_folder}'

    activity_id = get_max_value(table_name='activities',column_name='activity_id')
    description_id = get_max_value(table_name='amountdescriptions',column_name='amount_desc_id')

    final_activity_df = pd.DataFrame()
    performance_reports_df = pd.DataFrame()
    amount_description_df = pd.DataFrame()

    s3 = get_client('s3')
    response = s3.list_objects_v2(Bucket=source_bucket_name, Prefix=source_file_folder)

    if 'Contents' not in response or len(response['Contents']) <2:
        print("No files found in the specified folder.")
        return

    processed_files = []
    
    unique_id = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    for obj in response['Contents']:
        source_file_path = obj['Key']
        source_file_name = os.path.basename(source_file_path)
        if source_file_name.endswith('.pdf'):
            print(f"Processing file: {source_file_name}")


            response = get_response(client,source_bucket_name, source_file_path)
            doc = Document(response)

        
            table_1 = extract_table(doc, page_idx=0, table_idx=-1, skip_rows=1)
            table1_df, table2_df = process_dataframe(table_1,description_id)
            queries_result_df = extract_through_queries(client,source_bucket_name,source_file_path)

            queries_result_df['activity_id'] = activity_id
            table1_df['activity_id'] = activity_id

            description_id = int(table1_df['description_id'].max()) + 1
            activity_id+=1

            final_activity_df = pd.concat([final_activity_df,queries_result_df],ignore_index=True)
            performance_reports_df = pd.concat([performance_reports_df,table1_df],ignore_index=True)
            amount_description_df = pd.concat([amount_description_df,table2_df],ignore_index=True)

            processed_files.append(source_file_path)

    performance_reports_df = process_performance_reports_dataframe(performance_reports_df)
    final_activity_df = process_activity_dataframe(final_activity_df)
    amount_description_df = process_amount_description_dataframe(amount_description_df)

    save_to_s3(performance_reports_df, destination_bucket_name, f'{destination_folder}/performance_reports_{unique_id}.csv')
    save_to_s3(amount_description_df, destination_bucket_name, f'{destination_folder}/amount_description_{unique_id}.csv')
    save_to_s3(final_activity_df, destination_bucket_name, f'{destination_folder}/activity_{unique_id}.csv')


    for source_file_path in processed_files:
        source_file_name = os.path.basename(source_file_path)
        move_file_in_s3(source_bucket_name, source_file_path, source_bucket_name, f"{processed_folder}/{source_file_name}")