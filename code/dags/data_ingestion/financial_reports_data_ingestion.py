
from helpers.aws_utils import move_file_in_s3, load_s3_file,find_files_in_s3
from helpers.db_utils import write_to_postgres,get_max_value
import time



def ingest_financial_reports():
    
    source_bucket = "project-processed-data"
    source_folder_name = "financial_reports"
    destination_folder_name = 'archived'

    table_name = 'grantfinancialreports'

    files = find_files_in_s3(source_bucket, source_folder_name)
    

    
    for file in files:

        report_id = int(get_max_value(table_name,'report_id')) + 1

        print(f"Processing file: {file}")
        
        financial_report_df = load_s3_file(source_bucket, file)
        financial_report_df['report_id'] = list(range(report_id,len(financial_report_df)+ report_id))

        financial_report_df.loc[:,'grant_id'] = financial_report_df.loc[:,'grant_id'].str.strip()
        financial_report_df = financial_report_df.dropna(axis=1)

        success = write_to_postgres(financial_report_df, table_name)
        
        if success:
            move_file_in_s3(source_bucket, file, source_bucket, f'{destination_folder_name}/{file}')

        time.sleep(1)