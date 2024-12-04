import os
import pdfplumber
import re
from io import BytesIO
import us
import pandas as pd
from helpers.aws_utils import get_client, save_to_s3, move_file_in_s3  # Import functions from helper.py
from helpers.date_extractor import convert_dates,extract_date_from_filename,transform_date_column
from helpers.db_utils import fetch_existing_data_from_postgres
# Function to extract text from the PDF
def extract_text_from_pdf(pdf_bytes):
    text = ""
    with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            text += page.extract_text()
    return text

# Function to extract report date with adjustments for year length
def extract_report_date(text,file_name):
    report_date = ""
    heading_year = ""
    heading_year_match = re.search(r'\d{4}-\d{4}', text)
    if heading_year_match:
        heading_year = heading_year_match.group().split('-')[1]
    lines = text.split('\n')
    for line in lines:
        report_date_match = re.search(r'([A-Z][a-z]+ \d{1,2}, \d{1,5})', line)
        if report_date_match:
            report_date = report_date_match.group(1)
            report_date_parts = report_date.split(" ")
            year = report_date_parts[-1]
            if len(year) > 4 and heading_year:
                report_date = " ".join(report_date_parts[:-1]) + " " + heading_year
            elif len(year) < 4 and heading_year:
                report_date = " ".join(report_date_parts[:-1]) + " " + heading_year
            break
    
    report_date = convert_dates(report_date)

    if report_date=="" or report_date==None:
        report_date = extract_date_from_filename(file_name)

    return report_date



# Function to extract disaster events, P.L. numbers with dates, and Puerto Rico info
# Function to extract disaster events, P.L. numbers with dates, and Puerto Rico grant info
def extract_disaster_events_and_pl(text, report_date):
    # Define patterns for disaster events
    disaster_event_patterns = [
        r'([A-Za-z, ]+ \d{4}-\d{4})',        # Multi-year format
        r'([A-Za-z, ]+ \d{4})',              # Single-year format
        r'(\d{4})-(\d{4})\s+(.+)',           # Year range
        r'(\d{4})\s+(.+)',                   # Single-year format
        r'Hurricane[s]? [A-Za-z, &]+ \d{4}',  # Multiple hurricane names
        r'Hurricane [A-Za-z ]+ \d{4}',       # Single hurricane format
        r'Hurricanes [A-Za-z, ]+',           # Multiple hurricanes
    ]
    
    # Updated regex pattern to capture P.L. numbers and dates
    pl_date_pattern = r'(P\.?L\.?\s*\d{2,3}\s*-\s*\d{1,3})\s*([A-Za-z]{3,9}\.?\s*\d{1,2},?\s*\d{4}|[A-Za-z]{3,9}\.?\s*\d{1,2}(?:[\.\-]?\s*\d{4})?|(?:\w+\s*\d{1,2}(?:[\.\-]?\s*\d{4})?))?'
    
    # Ensure standalone P.L. numbers without dates are captured
    standalone_pl_pattern = r'(P\.?L\.?\s*\d{2,3}\s*-\s*\d{1,3})(?=\s*[^\d]|$)'

    # Split the text into lines
    lines = text.split('\n')
    
    disaster_event = None
    records = []
    puerto_rico_records = []  # List to hold Puerto Rico specific records
    
    # Loop through lines to capture disaster events and associate P.L. numbers
    for line in lines:
        line = line.strip()
        
        # Check if the line contains a disaster event
        for pattern in disaster_event_patterns:
            event_match = re.match(pattern, line)
            if event_match:
                disaster_event = event_match.group().strip()  # Update the current disaster event
                break
        
        # Check for P.L. numbers and dates
        pl_matches = re.findall(pl_date_pattern, line)
        if pl_matches and disaster_event:
            for pl_number, pl_date in pl_matches:
                records.append({
                    'Disaster Event': disaster_event,
                    'P.L Number': pl_number.strip(),
                    'P.L Date': pl_date.strip() if pl_date.strip() else None,
                })
         # Extract Grantee Name and Grant Award where State/Territory is Puerto Rico
        if 'PUERTO RICO' in line:
            parts = line.split('$')
            if len(parts) == 2:
                # Regex to find the Grantee Name after "Puerto Rico"
                grantee_match = re.search(r'(?<=PUERTO RICO\s)(.*?)(?=\s+\$)', line)
                if grantee_match:
                    grantee_name = grantee_match.group(0).strip()  # Extract Grantee Name
                else:
                    grantee_name = "PUERTO RICO"  # Default if no specific name found

                # Clean up grant award amount
                grant_award = re.sub(r'[^\d.]', '', parts[1].strip())  # Keep only digits and decimal point
                if grant_award:  # Ensure we have a valid number
                    puerto_rico_records.append({
                        'Disaster Event': disaster_event,
                        'State/Territory': 'PUERTO RICO',
                        'Grantee Name': grantee_name,
                        'Grant Award': float(grant_award)  # Convert to float for numeric consistency
                    })

    # Capture standalone P.L. numbers
    for line in lines:
        standalone_pl_matches = re.findall(standalone_pl_pattern, line)
        for pl_number in standalone_pl_matches:
            if pl_number.strip() not in [record['P.L Number'] for record in records]:
                records.append({
                    'Disaster Event': disaster_event,  # Use the last known disaster event
                    'P.L Number': pl_number.strip(),
                    'P.L Date': None  # No date available
                })

    # Create DataFrames with the extracted information
    df_pl = pd.DataFrame(records)
    df_puerto_rico = pd.DataFrame(puerto_rico_records)
    df_puerto_rico['report_date'] = report_date

    disaster_events = df_puerto_rico['Disaster Event'].unique().tolist()
    df_pl = df_pl[df_pl['Disaster Event'].isin(disaster_events)]

    # Drop the 'Report Date' column if it exists
    # This step is not necessary if 'Report Date' is not being added
    # df_pl.drop(columns=['Report Date'], inplace=True, errors='ignore')
    # df_puerto_rico.drop(columns=['Report Date'], inplace=True, errors='ignore')
    
    return df_pl, df_puerto_rico


def separate_event_year(event):
    # Match year or year range (e.g., 2021 or 2021-2022)
    match = re.search(r'(\d{4}(?:-\d{4})?)', event)
    if match:
        year = match.group(0)
        # Remove the year and any trailing spaces from the event to get the event name
        event_name = re.sub(r'\s*' + re.escape(year) + r'\s*$', '', event).strip()
        return pd.Series([event_name, year])
    return pd.Series([event, None])  # No year found

def remove_year(name):
    # Remove the year at the end of the disaster name
    return re.sub(r'\s+\b(19|20)\d{2}\b$', '', name)
    # Extract the year from the 'disaster_event' column
def extract_year(name):
    match = re.search(r'\b(19|20)\d{2}(-\d{4})?\b', name)
    return match.group(0) if match else None

def load_existing_data():
    disaster_df_existing = fetch_existing_data_from_postgres('disaster')
    grantee_df_existing = fetch_existing_data_from_postgres('grantee')
    grant_df_existing = fetch_existing_data_from_postgres('"granthistory"')  # Grant is a reserved keyword
    publiclaw_df_existing = fetch_existing_data_from_postgres('publiclaw')
    disaster_publiclaw_df_existing = fetch_existing_data_from_postgres('disasterpubliclaw')
    return (disaster_df_existing, grantee_df_existing, grant_df_existing,
            publiclaw_df_existing, disaster_publiclaw_df_existing)

def convert_3_tables_to_5(grant_history,disaster_events,public_law_table):

    (disaster_df_existing, grantee_df_existing, grant_df_existing,
    publiclaw_df_existing, disaster_publiclaw_df_existing) = load_existing_data()
    
    grant_history = grant_history.rename(columns={
        'Disaster Event': 'disaster_event',
        'State/Territory': 'state_code',
        'Grantee Name': 'grantee_name',
        'Grant Award': 'grant_award'
    })

    disaster_events = disaster_events.rename(columns={
        'Disaster Name': 'disaster_name',
        'Disaster Year': 'disaster_year',
        'P.L Number': 'pl_number'
    })

    public_law_table = public_law_table.rename(columns={
        'P.L Number': 'pl_number',
        'P.L Date': 'pl_date'
    })


    # Step 2: Clean and standardize data in grant_history
    grant_history['disaster_event'] = grant_history['disaster_event'].str.strip()
    grant_history['grantee_name'] = grant_history['grantee_name'].str.strip()
    grant_history['state_code'] = grant_history['state_code'].str.strip()

    grant_history.loc[:, 'state_code'] = grant_history['state_code'].apply(
        lambda x: us.states.lookup(x.strip()).abbr if pd.notna(x) and us.states.lookup(x.strip()) else x
    )

    # Remove the year from the 'disaster_event' in grant_history to match the 'disaster_name' in disaster_events

    grant_history['disaster_name'] = grant_history['disaster_event'].apply(remove_year)

    # Step 3: Create Grantee DataFrame
    # Combine existing grantees with new grantees
    grantee_new = grant_history[['grantee_name', 'state_code']].drop_duplicates()

    # Identify new grantees
    grantee_combined = pd.concat([grantee_df_existing[['grantee_name', 'state_code']], grantee_new])
    grantee_combined = grantee_combined.drop_duplicates().reset_index(drop=True)

    # Assign grantee_id
    grantee_combined['grantee_id'] = grantee_combined.index + 1

    # Merge back to grant_history to get grantee_id
    grant_history = grant_history.merge(grantee_combined, on=['grantee_name', 'state_code'], how='left')

    # Step 4: Create Disaster DataFrame from grant_history and disaster_events
    # Extract unique disasters from grant_history
    disasters_from_grant_history = grant_history[['disaster_name', 'disaster_event']].drop_duplicates()

    disasters_from_grant_history['disaster_year'] = disasters_from_grant_history['disaster_event'].apply(extract_year)

    # Extract disasters from disaster_events
    disasters_from_events = disaster_events[['disaster_name', 'disaster_year']].drop_duplicates()

    # Combine disasters from existing data, grant_history, and disaster_events
    disaster_combined = pd.concat([
        disaster_df_existing[['disaster_name', 'disaster_year']],
        disasters_from_grant_history[['disaster_name', 'disaster_year']],
        disasters_from_events
    ]).drop_duplicates().reset_index(drop=True)

    # Assign disaster_id
    disaster_combined['disaster_id'] = disaster_combined.index + 1

    # Merge back to grant_history to get disaster_id
    grant_history = grant_history.merge(disaster_combined[['disaster_name', 'disaster_id']], on='disaster_name', how='left')

    # Step 5: Create Public Law DataFrame
    # Combine existing public laws with new ones

    publiclaw_new = public_law_table[['pl_number', 'pl_date']].drop_duplicates() 
    publiclaw_new['pl_number'] = publiclaw_new['pl_number'].str.strip()
    publiclaw_new['pl_date'] = publiclaw_new['pl_date'].apply(transform_date_column)
    publiclaw_combined = pd.concat([publiclaw_df_existing, publiclaw_new]).drop_duplicates(subset=['pl_number']).reset_index(drop=True)

    # Step 6: Create DisasterPublicLaw DataFrame
    # Clean pl_number in disaster_events
    disaster_events['pl_number'] = disaster_events['pl_number'].str.strip()


    # Merge disaster_events with disaster_combined to get disaster_id
    disaster_publiclaw_new = disaster_events.merge(disaster_combined, on='disaster_name', how='left')

    # Remove entries where disaster_id is missing
    disaster_publiclaw_new = disaster_publiclaw_new.dropna(subset=['disaster_id'])

    # Merge with publiclaw_combined to ensure pl_number exists
    disaster_publiclaw_new = disaster_publiclaw_new.merge(publiclaw_combined, on='pl_number', how='left')

    # Remove entries where pl_number is missing in publiclaw_combined
    disaster_publiclaw_new = disaster_publiclaw_new.dropna(subset=['pl_number'])

    # Select required columns and remove duplicates
    disaster_publiclaw_combined = pd.concat([
        disaster_publiclaw_df_existing,
        disaster_publiclaw_new[['disaster_id', 'pl_number']]
    ]).drop_duplicates().reset_index(drop=True)

    # Step 7: Prepare Grant DataFrame
    # Combine existing grants with new ones
    grant_new = grant_history[['disaster_id', 'grantee_id', 'grant_award', 'report_date']].drop_duplicates()

    # Assign grant_history
    max_existing_grant_history = grant_df_existing['grant_history_id'].max() if not grant_df_existing.empty else 0
    grant_new = grant_new.reset_index(drop=True)
    grant_new['grant_history_id'] = grant_new.index + 1 + max_existing_grant_history

    # Combine grants
    grant_combined = pd.concat([grant_df_existing, grant_new]).drop_duplicates(subset=['grant_history_id']).reset_index(drop=True)


    return disaster_combined,grantee_combined,grant_combined,publiclaw_combined,disaster_publiclaw_combined

def process_history_reports():


    # Initialize the S3 client
    s3_client = get_client('s3')

    # S3 bucket and folder names
    source_bucket_name = 'project-raw-data-files'  # Correct source bucket for raw data
    destination_bucket_name = 'project-processed-data'         # Correct destination bucket for processed data
    source_folder = 'history_reports'            # Source folder in raw data bucket
    destination_folder = 'history_reports'              # Folder in processed data bucket
    archived_folder = f'archived/{destination_folder}'
    response = s3_client.list_objects_v2(Bucket=source_bucket_name, Prefix=source_folder)
    for obj in response.get('Contents', [])[-1 :]:
        source_file_path = obj['Key']
        source_file_name = os.path.basename(source_file_path)
        if source_file_path.endswith('.pdf'):
            # Extract the base file name without the folder path and file extension
            file_name = os.path.splitext(os.path.basename(source_file_path))[0]
            # Updated regex to also match YYYY-MM or YYYY_MM format
            match = re.search(r'(\d{4}[-_]\d{1,2})|(\d{1,2}[-_]\d{1,2}[-_]\d{4})', file_name)
            date_part = match.group().replace('-', '_') if match else 'unknown_date'

            # Fetch and process the PDF
            pdf_obj = s3_client.get_object(Bucket=source_bucket_name, Key=source_file_path)
            text = extract_text_from_pdf(pdf_obj['Body'].read())
            report_date = extract_report_date(text,file_name)
            df_pl, grant_history = extract_disaster_events_and_pl(text, report_date)

            # Separate Disaster Event Name and Year
            df_pl[['Disaster Name', 'Disaster Year']] = df_pl['Disaster Event'].apply(
                lambda event: pd.Series(separate_event_year(event))
            )
            disaster_events = df_pl[['Disaster Name', 'Disaster Year', 'P.L Number']]

            # Save to S3 using helper function with dynamic file naming
            public_law_table = df_pl[['P.L Number', 'P.L Date']].drop_duplicates()


            disaster_df,grantee_df,grant_df,publiclaw_df,disaster_publiclaw_df = convert_3_tables_to_5(grant_history,disaster_events,public_law_table)

            disaster_df_success = save_to_s3(
                disaster_df, destination_bucket_name, 
                f'{destination_folder}/disaster_events_{date_part}.csv'
            )


            grantee_df_success = save_to_s3(
                grantee_df, destination_bucket_name, 
                f'{destination_folder}/grantee_{date_part}.csv'
            )


            grant_df_success = save_to_s3(
                grant_df, destination_bucket_name, 
                f'{destination_folder}/grant_{date_part}.csv'
            )

            publiclaw_df_success  = save_to_s3(
                publiclaw_df, destination_bucket_name, 
                f'{destination_folder}/public_law_numbers_{date_part}.csv'
            )

            disaster_publiclaw_success = save_to_s3(
                disaster_publiclaw_df, destination_bucket_name, 
                f'{destination_folder}/disaster_public_law_{date_part}.csv'
            )

            if disaster_df_success and grantee_df_success and grant_df_success and publiclaw_df_success and disaster_publiclaw_success:
                # Move the processed file to another folder in the source bucket
                move_file_in_s3(source_bucket_name, source_file_path, source_bucket_name, f"{archived_folder}/{source_file_name}")

                print(f"Processed file saved to S3: s3://{destination_bucket_name}/{source_file_name}")
