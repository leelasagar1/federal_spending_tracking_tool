import boto3
import pandas as pd
import json
from io import StringIO


# Function to load AWS configuration from JSON file
def load_aws_config(config_file):
    with open(config_file, 'r') as file:
        return json.load(file)

# Function to get AWS client using loaded configuration
def get_client(service):

    # Get AWS configuration
    aws_config = load_aws_config('config/aws_config.json')

    return boto3.client(
        service,
        region_name=aws_config['region_name'],
        aws_access_key_id=aws_config['aws_access_key_id'],
        aws_secret_access_key=aws_config['aws_secret_access_key']
    )


def save_to_s3(data, bucket_name, output_file_key):
    try:
        csv_buffer = StringIO()
        data.to_csv(csv_buffer, index=False)
        s3_client = get_client('s3')
        s3_client.put_object(
                Bucket=bucket_name,
                Key=output_file_key,
                Body=csv_buffer.getvalue()
            )
        print(f"File saved to S3: s3://{bucket_name}/{output_file_key}")
        return True
    except Exception as e:
        print(f"Error occured while saving the file {output_file_key} to s3: {bucket_name}, Error:{e}")
        return False

    


def move_file_in_s3(source_bucket, source_key, destination_bucket, destination_key):
    s3 = get_client('s3')
    # Copy the file to the destination
    # print(destination_key)
    s3.copy_object(
        Bucket=destination_bucket,
        CopySource={'Bucket': source_bucket, 'Key': source_key},
        Key=destination_key
    )
    # Delete the original file
    s3.delete_object(Bucket=source_bucket, Key=source_key)
    print(f"File moved from s3://{source_bucket}/{source_key} to s3://{destination_bucket}/{destination_key}")




def load_s3_file(bucket, key):
    s3 = get_client('s3')
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(obj['Body'])

def find_files_in_s3(bucket, prefix):
    s3 = get_client('s3')
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    files_list = []
    for item in response.get('Contents', []):    
        if len(item['Key'].split('/')[1]) > 0:

            files_list.append(item['Key'])
    return files_list

