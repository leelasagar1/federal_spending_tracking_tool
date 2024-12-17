
from helpers.aws_utils import move_file_in_s3, load_s3_file,find_files_in_s3
from helpers.db_utils import write_to_postgres,get_max_value
from helpers.utils import find_file_by_keyword
import re
from collections import defaultdict

def get_unique_dates(files):
    date_pattern = re.compile(r'_(\d{1,2}_\d{1,2}_\d{4})\.csv$')
    dates = set()
    
    for file in files:
        match = date_pattern.search(file)
        if match:
            dates.add(match.group(1))
    
    return list(dates)

# Function to group files by date
def group_files_by_date(files, dates):
    grouped_files = defaultdict(list)
    
    for date in dates:
        date_pattern = f'_{date}\\.csv$'
        for file in files:
            if re.search(date_pattern, file):
                grouped_files[date].append(file)
                
    return grouped_files

# Main function to list, group, and process files
def ingest_history_reports():

    source_bucket = "project-processed-data"
    source_folder_name = "history_reports"
    destination_folder_name = 'archived'
    # List all files in the bucket
    all_files = find_files_in_s3(source_bucket,source_folder_name)
    
    # Get unique dates from the files
    unique_dates = get_unique_dates(all_files)
    
    # Group files by date
    grouped_files = group_files_by_date(all_files, unique_dates)

    files_to_table_dict = {
        'disaster_events':'disaster',
        'grantee':'grantee',
        'grant':'granthistory',
        'public_law_numbers': 'publiclaw',
        'disaster_public_law':'disasterpubliclaw'
        
    }
    # Process each group sequentially
    if len(grouped_files)> 0:
        for date, files in grouped_files.items():
            
            for file,table_name in files_to_table_dict.items():
                # Download the file to local storage
                file_name = find_file_by_keyword(files, keyword=file)
                if file_name:

                    df = load_s3_file(source_bucket, file_name)
                

                    print(f"Ingesting new data into {table_name}")
                    success = write_to_postgres(df, table_name=table_name)
                    if success:
                        move_file_in_s3(source_bucket, file_name, source_bucket, f'{destination_folder_name}/{file_name}')

    else:
        print('No files to ingest')