import pandas as pd

from helpers.aws_utils import move_file_in_s3, load_s3_file,find_files_in_s3
from helpers.db_utils import write_to_postgres
import time
# Connect to S3




def handle_null_dates(activities_df, date_column):
    # Convert the column to datetime, handling errors by coercing them to NaT
    activities_df[date_column] = pd.to_datetime(activities_df[date_column], errors='coerce')
    
    # Replace NaT values with None to be database-compatible
    activities_df[date_column] = activities_df[date_column].apply(lambda x: x if pd.notna(x) else None)

    return activities_df

def transform_performance_reports(file_1):
    return file_1.rename(columns={
        'performance_id': 'performance_id',
        'activity_id': 'activity_id',
        'description_id': 'amount_desc_id',
        'grant_id': 'grant_id',
        'to_date_amount': 'to_date_amount',
        'time_period_amount': 'time_period_amount'
    })

def transform_amount_descriptions(file_2):
    return file_2.rename(columns={
        'id': 'amount_desc_id',
        'description': 'description',
        'time_period_amount': 'time_period_amount',
        'time_period': 'time_period',
        'to_date_amount': 'to_date_amount'
    })

def transform_activities(file_3):
    activities_df = file_3.rename(columns={
        'activity_id': 'activity_id',
        'Grantee_Activity_Number': 'activity_number',
        'Activity_Type': 'activity_type',
        'Activity_Status': 'activity_status',
        'Project_Number': 'project_number',
        'Project_Title': 'project_title',
        'Projected_Start_Date': 'projected_start_date',
        'Projected_End_Date': 'projected_end_date',
        'National_Objective': 'national_objective',
        'Responsible_Organization': 'responsible_entity',
        'Benefit_Type': 'benefit_type',
        'Activity_Title': 'activity_title',
        'Completed_Activity_Actual_End_Date': 'completed_activity_actual_end_date'
    })

    activities_df = handle_null_dates(activities_df, 'projected_start_date')
    activities_df = handle_null_dates(activities_df, 'projected_end_date')
    activities_df = handle_null_dates(activities_df, 'completed_activity_actual_end_date')

    return activities_df



def ingest_performance_reports():
    
    source_bucket = "project-processed-data"
    source_folder_name = "quarterly_performance_reports"
    destination_folder_name = 'archived'


    files = find_files_in_s3(source_bucket, source_folder_name)
    
    # Determine the required files
    performance_file = next((file for file in files if 'performance_reports' in file.split('/')[1].lower()), None)
    amount_description_file = next((file for file in files if 'amount_description' in file.split('/')[1].lower()), None)
    activity_file = next((file for file in files if 'activity' in file.split('/')[1].lower()), None)

    # Load files from S3

    if amount_description_file:
        file_2 = load_s3_file(source_bucket, amount_description_file)
        amount_descriptions_df = transform_amount_descriptions(file_2)
        success = write_to_postgres(amount_descriptions_df, table_name='AmountDescriptions')
        
        if success:
            move_file_in_s3(source_bucket, amount_description_file, source_bucket, f'{destination_folder_name}/{amount_description_file}')
        time.sleep(1)
    if activity_file:
        file_3 = load_s3_file(source_bucket, activity_file)
        activities_df = transform_activities(file_3)
        success = write_to_postgres(activities_df, table_name='Activities')
        if success:
            move_file_in_s3(source_bucket, activity_file, source_bucket, f'{destination_folder_name}/{activity_file}')
        time.sleep(1)
    if performance_file:
        file_1 = load_s3_file(source_bucket, performance_file)
        performance_reports_df = transform_performance_reports(file_1)
        success = write_to_postgres(performance_reports_df, table_name='PerformanceReports')
        if success:
            move_file_in_s3(source_bucket, performance_file, source_bucket, f'{destination_folder_name}/{performance_file}')



