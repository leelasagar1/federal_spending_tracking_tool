import pandas as pd
import time
import re

from helpers.aws_utils import move_file_in_s3, load_s3_file,find_files_in_s3
from helpers.db_utils import write_to_postgres,fetch_existing_data_from_postgres
from helpers.utils import find_file_by_keyword
def get_latest_files(files):

    pattern = r'usa_spending_data/[a-z_]+_(\d{8}_\d{6})\.csv'

    # Extract all unique timestamps
    timestamps = set()
    for file in files:
        match = re.search(pattern, file)
        if match:
            timestamps.add(match.group(1))

    # Find the latest timestamp
    latest_timestamp = max(timestamps)

    # Find all files with the latest timestamp
    latest_files = [file for file in files if latest_timestamp in file]

    # Output the list of latest files
    return latest_files



def check_and_filter_new_data(df, table_name):
    """
    This function checks the data in the dataframe against the existing data in the database
    and filters out rows that are already present in the database.

    :param df: DataFrame containing the new data
    :param table_name: Name of the table in the database to check against
    :return: DataFrame containing only new rows that are not in the database
    """
    existing_data = fetch_existing_data_from_postgres(table_name)
    if table_name == 'transactions':
        existing_data.drop('transaction_id',axis=1,inplace=True)
        existing_data['action_date']= pd.to_datetime(existing_data["action_date"], errors="coerce").dt.strftime("%m-%d-%Y")

    elif table_name == 'federalawards':
        existing_data.drop('award_id',axis=1,inplace=True)
        existing_data['start_date']= pd.to_datetime(existing_data["start_date"], errors="coerce").dt.strftime("%m-%d-%Y")
        existing_data['end_date']= pd.to_datetime(existing_data["end_date"], errors="coerce").dt.strftime("%m-%d-%Y")
    elif table_name == 'funding':
        existing_data.drop('funding_id',axis=1,inplace=True)

    elif table_name == 'grants':
        df['awarding_agency_code'] = df['awarding_agency_code'].apply(lambda x: x if pd.isna(x) else str(int(x)))
        df['funding_agency_code'] = df['funding_agency_code'].apply(lambda x: x if pd.isna(x) else str(int(x)))

        existing_data['start_date']= pd.to_datetime(existing_data["start_date"], errors="coerce").dt.strftime("%m-%d-%Y")
        existing_data['end_date']= pd.to_datetime(existing_data["end_date"], errors="coerce").dt.strftime("%m-%d-%Y")

    if existing_data is not None and not existing_data.empty:
        # Ensure columns have the same data type before merging
        for column in df.columns:
            if column in existing_data.columns:

                if df[column].dtype != existing_data[column].dtype:
                    if df[column].dtype == 'float64' or existing_data[column].dtype == 'float64':
                        df[column] = df[column].astype('float64')
                        existing_data[column] = existing_data[column].astype('float64')
                    elif df[column].dtype == 'int64' or existing_data[column].dtype == 'int64':
                        df[column] = df[column].astype('float64')
                        existing_data[column] = existing_data[column].astype('float64')

        merged_df = df.merge(existing_data, how='left', indicator=True)
        new_data_df = merged_df[merged_df['_merge'] == 'left_only'].drop(columns=['_merge'])
        
        for col in new_data_df.columns:
            new_data_df[col] = new_data_df[col].replace('nan',pd.NA)
        return new_data_df
    else:
        return df



def ingest_usa_spending_data():
    source_bucket = "project-processed-data"
    source_folder_name = "usa_spending_data"
    destination_folder_name = 'archived'

    table_name = 'grantfinancialreports'

    files = find_files_in_s3(source_bucket, source_folder_name)
    latest_files = get_latest_files(files)

    files_to_table_dict = {
        'states':'states',
        'cfda':'cfdaprograms',
        'recipients':'recipients',
        'agency_data': 'agencies',
        'sub_agency':'subagency',
        'grants':'grants',
        'funding':'funding',
        "federal_awards":'federalawards',
        "transactions":'transactions',
        'sub_awards': 'subawards'
        
    }

    for file,table in files_to_table_dict.items():
        
        file_name = find_file_by_keyword(latest_files, keyword=file)
        if file_name:

            df = load_s3_file(source_bucket, file_name)
            print(f"Checking for new data to ingest into {table}")

            # Filter out data that already exists in the database
            new_data_df = check_and_filter_new_data(df, table)

            if not new_data_df.empty:
                print(f"Ingesting new data into {table}")
                success = write_to_postgres(new_data_df, table_name=table)
                if success:
                    move_file_in_s3(source_bucket, file_name, source_bucket, f'{destination_folder_name}/{file_name}')
            else:
                print(f"No new data to ingest for {table}")
                move_file_in_s3(source_bucket, file_name, source_bucket, f'{destination_folder_name}/{file_name}')