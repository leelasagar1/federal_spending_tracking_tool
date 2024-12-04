import requests
import os
import boto3
import pandas as pd
from datetime import datetime
import time
import logging
from requests.exceptions import RequestException
from helpers.aws_utils import save_to_s3


# Define constants
URL_SPENDING_BY_AWARD = "https://api.usaspending.gov/api/v2/search/spending_by_award/"
AWARD_DATA_URL = "https://api.usaspending.gov/api/v2/awards/"
FUNDING_URL = "https://api.usaspending.gov/api/v2/awards/funding/"
SUBAWARDS_URL = "https://api.usaspending.gov/api/v2/subawards/"
TRANSACTIONS_URL = "https://api.usaspending.gov/api/v2/transactions/"
S3_BUCKET = 'project-raw-data-files'
S3_FOLDER = 'usa_spending_data'


# Fetch all pages for a given endpoint

def fetch_all_pages(payload, url, sleep_time=0.2):
    all_results = []
    page = 1
    while True:
        payload["page"] = page
        try:
            response = requests.post(url, json=payload)
            response.raise_for_status()
            data = response.json()
            if 'results' not in data or len(data['results']) == 0:
                break
            all_results.extend(data['results'])
            page += 1
            time.sleep(sleep_time)
        except RequestException as e:
            logging.error(f"Error fetching page {page}: {e}")
            break
    return all_results

# Fetch award data for each award ID

def fetch_award_data(df, max_retries=3, sleep_time=2):
    """
    Fetch data from the USAspending API for each award ID in the DataFrame.

    Parameters:
    df (pd.DataFrame): DataFrame containing 'generated_internal_id' column with award IDs.
    max_retries (int): Maximum number of retries in case of API failure (default: 3).
    sleep_time (int): Time in seconds to sleep between API calls to avoid rate limiting (default: 2).

    Returns:
    pd.DataFrame: DataFrame containing the extracted data.
    """

    # Create a list to store extracted data
    all_extracted_data = []

    # Loop through each award_id in the 'generated_internal_id' column
    for index, award_id in enumerate(df['generated_internal_id']):
        url = f"https://api.usaspending.gov/api/v2/awards/{award_id}/"
        
        # Initialize retries
        retries = 0
        
        while retries < max_retries:
            try:
                # Log the request being made
                logging.info(f"Fetching data for award ID: {award_id} (Request {index + 1}/{len(df)})")
                
                # Make the GET request
                response = requests.get(url)
                
                # Check if the request was successful
                if response.status_code == 200:
                    # Parse the JSON response
                    data = response.json()
                    
                    # Extract the desired fields
                    extracted_data = {
                        "Award ID": data.get("fain"),  # Ensure this is extracted
                        "generated_unique_award_id": data.get("generated_unique_award_id"),
                        "total_funding": data.get("total_funding"),
                        "total_account_outlay": data.get("total_account_outlay"),
                        "total_account_obligation": data.get("total_account_obligation"),
                        "cfda_objectives": data["cfda_info"][0].get("cfda_objectives") if data.get("cfda_info") else None,
                        "cfda_title": data["cfda_info"][0].get("cfda_title") if data.get("cfda_info") else None,
                        "cfda_number": data["cfda_info"][0].get("cfda_number") if data.get("cfda_info") else None,
                        "place_of_performance_state_name": data["place_of_performance"].get("state_name") if data.get("place_of_performance") else None,
                        "place_of_performance_state_code": data["place_of_performance"].get("state_code") if data.get("place_of_performance") else None,
                        "total_outlay": data.get("total_outlay"),
                        "total_obligated": data.get("total_obligation"),
                        "awarding_agency_name": data["awarding_agency"].get("toptier_agency", {}).get("name") if data.get("awarding_agency") else None,
                        "awarding_agency_code": data["awarding_agency"].get("toptier_agency", {}).get("code") if data.get("awarding_agency") else None,
                        "awarding_sub_agency_name": data["awarding_agency"].get("subtier_agency", {}).get("name") if data.get("awarding_agency") else None,
                        "awarding_sub_agency_code": data["awarding_agency"].get("subtier_agency", {}).get("code") if data.get("awarding_agency") else None,
                        "funding_agency_name": data["funding_agency"].get("toptier_agency", {}).get("name") if data.get("funding_agency") else None,
                        "funding_agency_code": data["funding_agency"].get("toptier_agency", {}).get("code") if data.get("funding_agency") else None,
                        "funding_sub_agency_name": data["funding_agency"].get("subtier_agency", {}).get("name") if data.get("funding_agency") else None,
                        "funding_sub_agency_code": data["funding_agency"].get("subtier_agency", {}).get("code") if data.get("funding_agency") else None,
                        "recipient_state_name": data["recipient"].get("location", {}).get("state_name") if data.get("recipient") else None,
                        "recipient_state_code": data["recipient"].get("location", {}).get("state_code") if data.get("recipient") else None,
                        "funding_office_name": data["funding_agency"].get("office_agency_name") if data.get("funding_agency") else None,
                        "awarding_office_name": data["awarding_agency"].get("office_agency_name") if data.get("awarding_agency") else None,
                        "start_date": data.get("period_of_performance_start_date"),
                        "end_date": data.get("period_of_performance_current_end_date"), 
                    }

                    # Append the extracted data to the list
                    all_extracted_data.append(extracted_data)
                    
                    # Sleep between requests to avoid overwhelming the API
                    time.sleep(sleep_time)
                    
                    # Break out of the retry loop on success
                    break
                
                else:
                    # Log failure and retry
                    retries += 1
                    logging.warning(f"Request failed with status code {response.status_code} for award ID {award_id}. Retry {retries}/{max_retries}")
                    time.sleep(sleep_time * 2)

            except RequestException as e:
                retries += 1
                logging.error(f"Error fetching data for award ID {award_id}: {e}. Retry {retries}/{max_retries}")
                time.sleep(sleep_time * 2)
        
        if retries == max_retries:
            logging.error(f"Failed to fetch data for award ID {award_id} after {max_retries} attempts")

    # Convert the list of dictionaries into a DataFrame
    other_variables = pd.DataFrame(all_extracted_data)
    
    return other_variables

# Fetch funding data for each award ID

def fetch_funding_data(award_id):
    """Fetch all funding data for a given award_id.

    Args:
        award_id (str): The ID of the award to fetch funding data for.

    Returns:
        list: A list of funding records for the specified award_id.
    """
    url = "https://api.usaspending.gov/api/v2/awards/funding/"
    payload = {
        "award_id": award_id,
        "page": 1,
        "limit": 100,  # Higher limit to reduce the number of requests per award_id
        "sort": "reporting_fiscal_date",
        "order": "asc"
    }
    
    all_results = []

    while True:
        try:
            # Make the API request
            response = requests.post(url, json=payload)
            response.raise_for_status()  # Raise an exception for HTTP errors
            
            data = response.json()
            results = data.get('results', [])
            all_results.extend(results)
            
            # Check for pagination
            if len(results) < payload['limit']:
                break  # Exit if no more records
            else:
                payload['page'] += 1  # Increment page for the next request
            
            # Respect API rate limits
            time.sleep(0.1)
            
        except requests.exceptions.HTTPError as http_err:
            logging.error(f"HTTP error occurred for award_id {award_id}: {http_err}")
            break
        except Exception as err:
            logging.error(f"Other error occurred for award_id {award_id}: {err}")
            break

    return all_results

def get_all_funding_data(df_spending_by_award):
    """Retrieve funding data for all award_ids in the DataFrame.

    Args:
        df (pd.DataFrame): DataFrame containing award IDs.

    Returns:
        pd.DataFrame: DataFrame containing all funding data.
    """
    all_funding_data = []

    for award_id in df_spending_by_award['generated_internal_id']:
        logging.info(f"Fetching funding data for award_id: {award_id}")
        funding_data = fetch_funding_data(award_id)
        
        # Add award_id to each record for reference
        for record in funding_data:
            record['generated_internal_id'] = award_id
        
        all_funding_data.extend(funding_data)

    
    federal_account_funding_df = pd.DataFrame(all_funding_data)
    federal_account_funding_df = federal_account_funding_df.merge(
            df_spending_by_award[['generated_internal_id', 'Award ID']], 
            on='generated_internal_id', 
            how='left'
        )

    return federal_account_funding_df

# Fetch subawards for each award ID

def fetch_and_process_subawards(award_id, limit=100):
    """Retrieve all subawards for a specific award_id and add internal ID.

    Args:
        award_id (str): The ID of the award to fetch subawards for.
        limit (int): Number of records per page.

    Returns:
        list: A list of all subawards for the specified award_id with internal ID added.
    """
    all_results = []
    page = 1
    url = "https://api.usaspending.gov/api/v2/subawards/"

    while True:
        payload = {
            "award_id": award_id,
            "page": page,
            "sort": "subaward_number",
            "order": "desc",
            "limit": limit
        }

        try:
            response = requests.post(url, json=payload)
            response.raise_for_status()
            data = response.json()
            results = data.get('results', [])
            has_next = data['page_metadata'].get('hasNext', False)
            next_page = data['page_metadata'].get('next', None)
        
            if not results:
                break

            for result in results:
                result['generated_internal_id'] = award_id

            all_results.extend(results)
            
            if not has_next:
                break
            page = next_page
            time.sleep(0.1)  # Respect the API's rate limits

        except requests.exceptions.RequestException as err:
            logging.error(f"Error occurred while fetching subawards for award_id {award_id}: {err}")
            break

    return all_results

def fetch_all_subawards(df):
    """Fetch subawards for all award IDs in the provided DataFrame and save to S3.

    Args:
        df (pd.DataFrame): DataFrame containing 'generated_internal_id' column.
        bucket_name (str): S3 bucket name.
        folder_name (str): Folder name in S3 bucket.
    """
    all_award_results = []
    award_ids = df['generated_internal_id'].unique()

    for award_id in award_ids:
        logging.info(f"Fetching subawards for award_id: {award_id}")
        award_results = fetch_and_process_subawards(award_id)
        if award_results:
            all_award_results.extend(award_results)

    sub_award_df = pd.DataFrame(all_award_results)

    if not sub_award_df.empty and 'id' in sub_award_df.columns:
        sub_award_df.rename(columns={'id': 'subaward_id'}, inplace=True)

    if sub_award_df.empty:
        logging.warning("No data to save. DataFrame is empty.")
        return

    return sub_award_df



def fetch_transactions(df, max_retries=3, delay_between_requests=0.2):
    """Fetch transaction data for each award_id in the provided DataFrame.
    
    Args:
        df (pd.DataFrame): DataFrame containing 'generated_internal_id' column.
        max_retries (int): Maximum number of retries for failed requests.
        delay_between_requests (float): Delay in seconds between requests.
    
    Returns:
        pd.DataFrame: DataFrame containing transaction data or an empty DataFrame if no records found.
    """
    url = "https://api.usaspending.gov/api/v2/transactions/"
    all_results = []  # Initialize an empty list to store all the results

    # Extract unique award IDs to avoid redundant requests
    unique_award_ids = df['generated_internal_id'].unique()

    for award_id in unique_award_ids:
        logging.info(f"Fetching transactions for award_id: {award_id}")

        # Initialize payload for each award_id
        payload = {
            "award_id": award_id,
            "page": 1,
            "sort": "modification_number",
            "order": "asc",
            "limit": 100  # Set a reasonable limit for each request
        }

        retries = 0
        while retries < max_retries:
            try:
                # Send a POST request to the API
                response = requests.post(url, json=payload)
                response.raise_for_status()

                # Parse the JSON response
                data = response.json()
                results = data.get('results', [])

                # Append the results to the all_results list, adding the award_id as a new column
                for result in results:
                    result['generated_internal_id'] = award_id
                all_results.extend(results)

                # Check if there are more pages of results
                if len(results) < payload["limit"]:
                    break  # No more pages, exit the loop
                else:
                    # Move to the next page
                    payload["page"] += 1

                # Respect API rate limits by adding a short delay between requests
                time.sleep(delay_between_requests)

            except requests.exceptions.RequestException as e:
                retries += 1
                logging.error(f"Request failed: {e}")
                if retries < max_retries:
                    logging.info(f"Retrying... ({retries}/{max_retries})")
                    time.sleep(delay_between_requests)
                else:
                    logging.error(f"Max retries reached for award_id {award_id}. Skipping.")
                    break

    # Convert the results into a pandas DataFrame if there are any results
    if all_results:
        transactions_df = pd.DataFrame(all_results)
        logging.info("Transactions data fetched successfully.")
    else:
        logging.info("No records found.")
        transactions_df = pd.DataFrame()

    # Merge the transactions with the original DataFrame to add the 'Award ID' column
    if not transactions_df.empty:
        transactions_df = pd.merge(
            transactions_df, 
            df[['generated_internal_id', 'Award ID']].drop_duplicates(),  # Select only 'generated_internal_id' and 'Award ID', drop duplicates
            on='generated_internal_id',  # Merge on this column
            how='left'  # Use left join to ensure all rows from transactions_df are kept
        )

    return transactions_df



# Main function to extract data

def extract_usa_spending_data():
    # Set the start date and dynamically set the end date as today's date
    start_date = "2007-10-01"
    end_date = datetime.today().strftime('%Y-%m-%d')
    payload_spending_by_award = {
        "fields": [
            "Award ID", "Recipient Name", "Award Amount", "Total Outlays", "Description", 
            "Award Type", "def_codes", "COVID-19 Obligations", "COVID-19 Outlays", 
            "Infrastructure Obligations", "Infrastructure Outlays", "Awarding Agency", 
            "Awarding Sub Agency", "Start Date", "End Date", "recipient_id", 
            "prime_award_recipient_id"
        ],
        "filters": {
            "time_period": [
                {"start_date": start_date, "end_date": end_date}
            ],
            "agencies": [{"type": "awarding", "tier": "toptier", "name": "Department of Housing and Urban Development"}],
            "award_type_codes": ["02", "03", "04", "05"],
            "place_of_performance_locations": [{"state": "PR", "country": "USA"}],
            "program_numbers": ["14.218", "14.228", "14.248"]
        },
        "limit": 100,
        "page": 1,
        "sort": "Award Amount",
        "order": "desc",
        "subawards": False
    }
    # return 
    # Fetch spending data
    print("Extracting Spending Data")
    results = fetch_all_pages(payload_spending_by_award, URL_SPENDING_BY_AWARD)
    df_spending_by_award = pd.DataFrame(results)
    file_name = f"{S3_FOLDER}/spending_by_award_{datetime.today().strftime('%m_%d_%Y')}.csv"
    save_to_s3(df_spending_by_award, S3_BUCKET, file_name)

    # Fetch and save award data
    print("Extracting Awards Data")
    df_award_data = fetch_award_data(df_spending_by_award)
    file_name = f"{S3_FOLDER}/other_data_{datetime.today().strftime('%m_%d_%Y')}.csv"
    save_to_s3(df_award_data, S3_BUCKET, file_name)

    # Fetch and save funding data
    print("Extracting Funding Data")
    df_funding_data = get_all_funding_data(df_spending_by_award)
    file_name = f"{S3_FOLDER}/funding_data_{datetime.today().strftime('%m_%d_%Y')}.csv"
    save_to_s3(df_funding_data, S3_BUCKET, file_name)

    # Fetch and save subaward data
    print("Extracting Subawards Data")
    df_subawards = fetch_all_subawards(df_spending_by_award)
    file_name = f"{S3_FOLDER}/subawards_{datetime.today().strftime('%m_%d_%Y')}.csv"
    save_to_s3(df_subawards, S3_BUCKET, file_name)

    # Fetch and save transaction data
    print("Extracting Transactions Data")
    df_transactions = fetch_transactions(df_spending_by_award)
    file_name = f"{S3_FOLDER}/transactions_{datetime.today().strftime('%m_%d_%Y')}.csv"
    save_to_s3(df_transactions, S3_BUCKET, file_name)

