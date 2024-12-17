from datetime import datetime
import os
import pandas as pd

from helpers.aws_utils import get_client,save_to_s3,load_s3_file,move_file_in_s3,find_files_in_s3


from datetime import datetime


import uuid

# def convert_date(date_string):
#     try:
#         return datetime.strptime(date_string, "%Y-%m-%d").strftime("%Y-%m-%d")
#     except (ValueError, TypeError):
#         return ""


def get_states_data(grants_df):

    result = (
        grants_df[["recipient_state_name", "recipient_state_code"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    result = result.rename(columns = { 'recipient_state_name':'state_name', 
                                      'recipient_state_code':'state_code'})
    return result


def get_cfda_programs_data(grants_df):
    result = (
        grants_df[["cfda_number", "cfda_title", "cfda_objectives"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )

    return result


def get_grants_data(awards_df, other_df,mapping_df):

    # Select only the required columns from df_spending_by_award for merging
    awards_selected_df = awards_df[
        [
            "generated_internal_id",
            "Award Type",
            "def_codes",
            "Description",
            "COVID-19 Obligations",
            "COVID-19 Outlays",
            "Infrastructure Outlays",
            "Infrastructure Obligations"
        ]
    ]

    # Merge the dataframes based on generated_unique_award_id and generated_internal_id
    result = other_df.merge(
        awards_selected_df,
        left_on="generated_unique_award_id",
        right_on="generated_internal_id",
        how="left",
    )

    # awarding_agency_mapping = funding_df.set_index("awarding_agency_name")["awarding_agency_id"].to_dict()
    # funding_agency_mapping = funding_df.set_index("funding_agency_name")["funding_agency_id"].to_dict()
    
    # result["awarding_agency_id"] = result['awarding_agency_name'].map(awarding_agency_mapping)
    # result["funding_agency_id"] = result['funding_agency_name'].map(funding_agency_mapping)

    # Drop the redundant 'generated_internal_id' column from the merged DataFrame
    result = result.drop(columns=["generated_internal_id"])

    result = result.rename(
        columns={
            "Award ID": "grant_id",
            "generated_unique_award_id": "generated_internal_id",
            "Description": "description",
            "Award Type": "award_type",
            "COVID-19 Obligations": "covid_19_obligations",
            "COVID-19 Outlays": "covid_19_outlays",
            "Infrastructure Outlays": "infrastructure_outlays",
            "Infrastructure Obligations":"infrastructure_obligations"
        }
    )

    # result["awarding_agency_id"] = result["awarding_agency_id"].fillna("")
    # result["funding_agency_id"] = result["funding_agency_id"].fillna("")
    # result["awarding_sub_agency_code"] = result["awarding_sub_agency_code"].fillna("")
    # result["funding_sub_agency_code"] = result["funding_sub_agency_code"].fillna("")
    # result["total_outlay"] = result["total_outlay"].fillna(0)
    # result["total_obligated"] = result["total_obligated"].fillna(0)
    # result["covid_19_obligations"] = result["covid_19_obligations"].fillna(0)
    # result["covid_19_outlays"] = result["covid_19_outlays"].fillna(0)
    # result["infrastructure_outlays"] = result["infrastructure_outlays"].fillna(0)
    # result["infrastructure_obligations"] = 0

    # Convert date columns to the appropriate format
    # result["start_date"] = result["start_date"].apply(convert_date)
    # result["end_date"] = result["end_date"].apply(convert_date)
 
    result["start_date"] = pd.to_datetime(result["start_date"], errors="coerce").dt.strftime("%m-%d-%Y")
    result["end_date"] = pd.to_datetime(result["end_date"], errors="coerce").dt.strftime("%m-%d-%Y")

    result['def_codes'] = result['def_codes'].apply(lambda x: x.strip("[]'\"") if not pd.isna(x) else x)

    def_codes_mapping_dict = mapping_df.set_index('def_codes')['PL Number 1'].to_dict()
    result['public_law_number'] = result['def_codes'].map(def_codes_mapping_dict)
    result['cfda_number'] = result['cfda_number'].astype(str)
    columns = [
        "grant_id",
        "generated_internal_id",
        "cfda_number",
        "place_of_performance_state_code",
        "recipient_state_code",
        "awarding_agency_code",
        "funding_agency_code",
        "awarding_sub_agency_code",
        "funding_sub_agency_code",
        "total_funding",
        "total_account_outlay",
        "total_account_obligation",
        "total_outlay",
        "total_obligated",
        "description",
        "award_type",
        "def_codes",
        "public_law_number",
        "covid_19_obligations",
        "covid_19_outlays",
        "infrastructure_obligations",
        "infrastructure_outlays",
        "start_date",
        "end_date",
        "funding_office_name",
        "awarding_office_name",
    ]
    result = result[columns]

    return result


def get_sub_awards_data(sub_awards_df, awards_df,recipients_df):

    result = sub_awards_df.merge(
        awards_df[["generated_internal_id", "Award ID"]],
        on="generated_internal_id",
        how="left",
    )
    result.rename(columns={
        'subaward_id': 'subaward_id',
        'subaward_number': 'subaward_number',
        'description': 'description',
        'action_date': 'action_date',
        'amount': 'amount',
        'recipient_name': 'recipient_name',
        'Award ID': 'grant_id'
    }, inplace=True)

    result['recipient_name'] = result['recipient_name'].str.strip()
    # Merge subaward data with recipient data to get recipient_id
    result = result.merge(recipients_df, how='left', left_on='recipient_name', right_on='recipient_name')


    # Convert action_date to datetime format
    result['action_date'] = pd.to_datetime(result['action_date']).dt.strftime('%m-%d-%Y')

    # Select and reorder the columns to match the target table structure
    result = result[['subaward_id', 'grant_id', 'subaward_number', 'description', 'action_date', 'amount', 'recipient_id']]
    
    return result


def get_recipients_data(awards_df,sub_awards_df):

    result = awards_df[["Recipient Name", "recipient_id"]].drop_duplicates()

    result = result.rename(columns={
        "Recipient Name": 'recipient_name'
    })

    result['recipient_name'] = result['recipient_name'].str.strip()

    combined_df = pd.concat([
        awards_df[['Recipient Name']].rename(columns={
            'Recipient Name': 'recipient_name'}),
        sub_awards_df[['recipient_name']]  ])
    
    # Drop duplicates to keep only unique name-code pairs
    result = combined_df.drop_duplicates().dropna().reset_index(drop=True)
    result['recipient_id'] = [str(uuid.uuid4()) for _ in range(len(result))]
    
    return result


def get_funding_data(funding_df):

    # result = funding_df.merge(
    #     awards_df[["generated_internal_id", "Award ID"]],
    #     on="generated_internal_id",
    #     how="left",
    # )

    
    result = funding_df.rename(columns={
        'Award ID': 'grant_id',
        # 'disaster_emergency_fund_code':'def_codes'
    })
    
    # Select all columns to align with the table
    result = result[['grant_id', 'transaction_obligated_amount', 'gross_outlay_amount', #'def_codes',
             'federal_account', 'account_title', #'funding_agency_id', 'awarding_agency_id', 
             'object_class', 'object_class_name', 'program_activity_code', 'program_activity_name', 'reporting_fiscal_year',
             'reporting_fiscal_quarter', 'reporting_fiscal_month', 'is_quarterly_submission',
             #'awarding_agency_slug', 'funding_agency_slug'
             ]]
    
    #Fill NaN values with appropriate defaults

    # result = result.fillna({
    #     'transaction_obligated_amount': 0,
    #     'gross_outlay_amount': 0,
    #     'disaster_emergency_fund_code': '',
    #     'federal_account': '',
    #     'account_title': '',
    #     'funding_agency_id': '',
    #     'awarding_agency_id': '',
    #     'object_class': '',
    #     'object_class_name': '',
    #     'program_activity_code': '',
    #     'program_activity_name': '',
    #     'reporting_fiscal_year': 0,
    #     'reporting_fiscal_quarter': 0,
    #     'reporting_fiscal_month': 0,
    #     'is_quarterly_submission': False,
    #     'awarding_agency_slug': '',
    #     'funding_agency_slug': ''
    # })
    #result['def_codes'] = result['def_codes'].apply(lambda x: x.strip("[]'\"") if not pd.isna(x) else x)
    #Enforce correct data types
    result['reporting_fiscal_year'] = result['reporting_fiscal_year'].astype(int)
    result['reporting_fiscal_quarter'] = result['reporting_fiscal_quarter'].astype(int)
    result['reporting_fiscal_month'] = result['reporting_fiscal_month'].astype(int)
    result['is_quarterly_submission'] = result['is_quarterly_submission'].astype(bool)



    return result


def get_sub_agency_data(other_df):

    combined_df = pd.concat([
        other_df[['awarding_sub_agency_name', 'awarding_sub_agency_code']].rename(columns={
            'awarding_sub_agency_name': 'sub_agency_name', 'awarding_sub_agency_code': 'sub_agency_code'}),
        other_df[['funding_sub_agency_name', 'funding_sub_agency_code']].rename(columns={
            'funding_sub_agency_name': 'sub_agency_name', 'funding_sub_agency_code': 'sub_agency_code'})
    ])
    # Drop duplicates to keep only unique name-code pairs
    result = combined_df.drop_duplicates().dropna().reset_index(drop=True)
    result['sub_agency_code'] = result['sub_agency_code'].astype(int)

    return result


def get_agency_data(funding_df,other_df):

    other_combined_df = pd.concat([
                other_df[['awarding_agency_name', 'awarding_agency_code']].rename(columns={
                    'awarding_agency_name': 'agency_name', 'awarding_agency_code': 'agency_code'}),
                other_df[['funding_agency_name', 'funding_agency_code']].rename(columns={
                    'funding_agency_name': 'agency_name', 'funding_agency_code': 'agency_code'})
                    ])
    # Drop duplicates to keep only unique name-code pairs
    other_combined_df = other_combined_df.drop_duplicates().dropna().reset_index(drop=True)   

    funding_combined_df = pd.concat([
            funding_df[['funding_agency_name', 'funding_agency_id', 'funding_toptier_agency_id', 'funding_agency_slug']].rename(columns={
                'funding_agency_name': 'agency_name', 
                'funding_agency_id': 'agency_id', 
                'funding_toptier_agency_id': 'toptier_agency_id',
                'funding_agency_slug': 'agency_slug'
            }),
            funding_df[['awarding_agency_name', 'awarding_agency_id', 'awarding_toptier_agency_id', 'awarding_agency_slug']].rename(columns={
                'awarding_agency_name': 'agency_name', 
                'awarding_agency_id': 'agency_id', 
                'awarding_toptier_agency_id': 'toptier_agency_id',
                'awarding_agency_slug': 'agency_slug'
            })
        ])

    # Drop duplicates to keep only unique name-id pairs
    funding_combined_df = funding_combined_df.drop_duplicates().dropna().reset_index(drop=True)

    result = pd.merge(
            other_combined_df, 
            funding_combined_df, 
            how='outer', 
            on='agency_name'
            )
    
    result['agency_code'] = result['agency_code'].astype(int)
    result['agency_id'] = result['agency_id'].astype(int)
    result['toptier_agency_id'] = result['toptier_agency_id'].astype(int)

    return result


def get_transactions_data(transactions_df):

    result = transactions_df.drop(columns=["id", "generated_internal_id"])


    # result['transaction_id'] = range(1, len(result) + 1)
    result['action_date'] = pd.to_datetime(result["action_date"], errors="coerce").dt.strftime("%m-%d-%Y")

    # Select and rename columns to match the target table format
    result = result.rename(columns={
        'Award ID': 'grant_id'
    })

    # Select the relevant columns
    result = result[[
        # 'transaction_id',
        'grant_id',
        'type',
        'type_description',
        'action_date',
        'action_type',
        'action_type_description',
        'modification_number',
        'description',
        'federal_action_obligation',
        'face_value_loan_guarantee',
        'original_loan_subsidy_cost',
        'cfda_number'
    ]]

    result['cfda_number'] = result['cfda_number'].astype(str)

    return result


def get_federal_awards_data(awards_df,recipient_df):

    # result = awards_df.merge(
    #     sub_agency_df[["awarding_sub_agency_name", "awarding_sub_agency_code"]].rename(
    #         columns={"awarding_sub_agency_name": "Awarding Sub Agency"}
    #     ),
    #     on="Awarding Sub Agency",
    #     how="left",
    # )
    # result = result.drop(
    #     [
    #         "internal_id",
    #         "Recipient Name",
    #         "Awarding Agency",
    #         "Awarding Sub Agency",
    #         "prime_award_recipient_id",
    #         "generated_internal_id",
    #     ],
    #     axis=1,
    # )
    
    # Rename columns to have consistent and clear names
    result = awards_df.rename(
        columns={
            "Award ID": "grant_id",
            "Award Amount": "award_amount",
            "Total Outlays": "total_outlays",
            "Description": "description",
            "Award Type": "award_type",
            "Recipient Name": 'recipient_name',
            "COVID-19 Obligations": "covid_19_obligations",
            "COVID-19 Outlays": "covid_19_outlays",
            "Infrastructure Obligations": "infrastructure_obligations",
            "Infrastructure Outlays": "infrastructure_outlays",
            "Start Date": "start_date",
            "End Date": "end_date",
                    }
                        )

    # Update the renamed columns with the correct data types
    result["award_amount"] = result["award_amount"].astype(float)
    result["total_outlays"] = result["total_outlays"].astype(float)
    result["covid_19_obligations"] = pd.to_numeric(result["covid_19_obligations"], errors="coerce")
    result["covid_19_outlays"] = pd.to_numeric(result["covid_19_outlays"], errors="coerce")
    result["infrastructure_obligations"] = pd.to_numeric(result["infrastructure_obligations"], errors="coerce")
    result["infrastructure_outlays"] = pd.to_numeric(result["infrastructure_outlays"], errors="coerce")
    # result["awarding_agency_id"] = result["awarding_agency_id"].astype(int)
    # result["awarding_sub_agency_code"] = result["awarding_sub_agency_code"].astype(int)
    result["start_date"] = pd.to_datetime(result["start_date"], errors="coerce").dt.strftime("%m-%d-%Y")
    result["end_date"] = pd.to_datetime(result["end_date"], errors="coerce").dt.strftime("%m-%d-%Y")
    # result['def_codes'] = result['def_codes'].apply(lambda x: x.strip("[]'\"") if not pd.isna(x) else x)

    recipient_mapping_dict = recipient_df.set_index('recipient_name')['recipient_id'].to_dict()
    result['recipient_id'] = result['recipient_name'].map(recipient_mapping_dict)
    # Selecting only the required columns for output
    result = result[
        [
            #"award_id",
            "grant_id",
            "recipient_id",
            "award_amount",
            "total_outlays",
            "description",
            "award_type",
            # "def_codes",
            "covid_19_obligations",
            "covid_19_outlays",
            "infrastructure_obligations",
            "infrastructure_outlays",
            # "awarding_agency_id",
            # "awarding_sub_agency_code",
            "start_date",
            "end_date",
            # "agency_slug",
        ]
    ]

    return result

def save_dfs_to_s3(dfs, bucket, folder, unique_id):

    for df, name in dfs:
        save_to_s3(df, bucket, f"{folder}/{name}_{unique_id}.csv")


def archive_files_in_s3(s3, response, bucket_name, archived_folder):
    for obj in response["Contents"]:
        source_file_path = obj["Key"]
        source_file_name = os.path.basename(source_file_path)
        move_file_in_s3(
            bucket_name,
            source_file_path,
            bucket_name,
            f"{archived_folder}/{source_file_name}",
        )



def process_usa_spending_data():

    source_bucket_name = "project-raw-data-files"
    source_file_folder = "usa_spending_data"
    destination_bucket_name = "project-processed-data"  # The bucket where processed files will be saved

    destination_folder = "usa_spending_data"  # Destination folder inside the destination bucket
    
    raw_archived_folder = f"archived/{destination_folder}"
    mapping_folder = 'mapping_files'
    mapping_file_name = 'def_codes_mapping_file.csv'
    
    s3 = get_client("s3")
    response = s3.list_objects_v2(Bucket=source_bucket_name, Prefix=source_file_folder)


    if 'Contents' not in response or len(response['Contents']) <2:
        print("No files found in the specified folder.")
        return
    
    mapping_df = load_s3_file(bucket=source_bucket_name,key=f'{mapping_folder}/{mapping_file_name}')

    for obj in response["Contents"]:
        source_file_path = obj["Key"]
        source_file_name = os.path.basename(source_file_path)
        if source_file_name.endswith(".csv"):
            print(f"Processing file: {source_file_name}")
            file_key = f"{source_file_folder}/{source_file_name}"
            if "spending_by_award" in source_file_name:
                awards_df = load_s3_file(bucket=source_bucket_name, key=file_key)

            if "subaward" in source_file_name:
                sub_awards_df = load_s3_file(bucket=source_bucket_name, key=file_key)

            if "transactions" in source_file_name:
                transactions_df = load_s3_file(bucket=source_bucket_name, key=file_key)

            if "funding" in source_file_name:
                funding_df = load_s3_file(bucket=source_bucket_name, key=file_key)

            if "other" in source_file_name:
                other_df = load_s3_file(bucket=source_bucket_name, key=file_key)

    states_df = get_states_data(other_df)
    cfda_df = get_cfda_programs_data(other_df)
    agency_df = get_agency_data(funding_df, other_df)
    sub_agency_df = get_sub_agency_data(other_df)
    recipients_df = get_recipients_data(awards_df,sub_awards_df)
    grants_df = get_grants_data(awards_df,other_df,mapping_df)
    federal_awards_df = get_federal_awards_data(awards_df,recipients_df)
    funding_table_df = get_funding_data(funding_df)
    transactions_table_df = get_transactions_data(transactions_df)
    sub_awards_table_df = get_sub_awards_data(sub_awards_df, awards_df,recipients_df)

    unique_id = datetime.now().strftime("%Y%m%d_%H%M%S")

    save_dfs_to_s3(
        [
            (states_df, "states"),
            (cfda_df, "cfda"),
            (agency_df, "agency_data"),
            (sub_agency_df, "sub_agency"),
            (recipients_df, "recipients"),
            (grants_df, "grants"),
            (federal_awards_df, "federal_awards"),
            (funding_table_df, "funding"),
            (transactions_table_df, "transactions"),
            (sub_awards_table_df, "sub_awards"),
        ],
        destination_bucket_name,
        destination_folder,
        unique_id,
    )

    archive_files_in_s3(s3, response, source_bucket_name, raw_archived_folder)


