import json
import time
import os
from collections import defaultdict
from datetime import timedelta, datetime
import csv
import re
import pandas as pd
import logging
import boto3
from botocore.exceptions import ClientError
import traceback
from io import BytesIO, StringIO
from botocore.config import Config
import requests
# Initialize the logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)
environment = "dev"
hostname = os.environ["hostname"]
url = os.environ["url"]
url_token = os.environ["url_token"]
secret_id = os.environ["secret_id"]
topic_arn = os.environ["lfc_topic_arn"]
bucket_name=os.environ["store_lookup_bucket"]
output_bucket= os.environ["output_bucket"]
input_folder = os.environ["input_folder"]
lookup_s3_key = os.environ["lookup_s3_key"]

# Set Retries for Boto3
config = Config(retries={'max_attempts': 10, 'mode': 'standard'})

# Create S3 Connection
s3 = boto3.client('s3', config=config)



# Initialize the secrets variable
secrets = None

def get_secrets():
    global secrets
    if secrets is not None:
        # If secrets are already retrieved, return them
        return secrets
    secret_name = "dev/cred"
    region_name = "us-east-1"
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # Handle the exception
        raise e
    # Parse the secret string as JSON
    secrets = json.loads(get_secret_value_response['SecretString'])
    return secrets
# Retrieve secrets
secrets = get_secrets()

def validate_json(json_input):
    '''
    Validate JSON string or dictionary.
 
    :param json_input: JSON in string or dictionary format
    :return: True if valid, False otherwise
    '''
    if isinstance(json_input, str):
        try:
            json.loads(json_input)
        except json.JSONDecodeError:
            return False
    elif isinstance(json_input, dict):
        # No need to convert, just validate the dictionary
        pass
    else:
        # If not a string or dictionary, it's invalid
        return False
 
    return True
def process_json_string_list(json_data_list):
    grouped_data = defaultdict(list)

    for json_data in json_data_list:
        for org_job_entry in json_data.get('orgJobs', []):
            org_job = org_job_entry.get('orgJob', {}).get('qualifier', '')
            headcounts_per_day = org_job_entry.get('headcountsPerDay', [])

            # Add the data to the grouped_data dictionary
            grouped_data[org_job].extend(headcounts_per_day)

    return grouped_data
def round_time_to_15_minutes(time):
    return (datetime.combine(datetime.today(), time) + timedelta(minutes=15)).time()

def post_chunk_to_Wfd(chunk_json_data, store_code, access_token=None):
    retries = 0
    chunk_json_data = json.loads(chunk_json_data)
    max_retries = 1
 
    while retries <= max_retries:
        try:
            print(f"access_token_2:{access_token}")
            api = f"{hostname}{url}"  # Remove one 'https://'
            logger.error(f"API Endpoint: {api}")
            headers = {
                "Authorization": access_token,
                "appkey": secrets["appkey"],
                "Content-Type": "application/json"
            }
            response = requests.post(api, headers=headers, json=chunk_json_data)
            if response.status_code == 200:
                logger.info("Labor Forecast created successfully")
                # Add your logic for handling a successful response
                break  # Exit the loop after a successful response
            elif response.status_code == 207:
                logger.info("Partial success in creating labor forecasts")
                # Add your logic for handling partial success
            else:
                if not validate_json(chunk_json_data):
                    logger.error("Invalid JSON in the request.")
                    break  # Exit the loop if the JSON is not valid
                handle_Wfd_error(response)
        except Exception as e:
            logger.exception("Exception during main program execution. The stack trace is as follows:" + traceback.format_exc())
            handle_retry_exception(e, retries, store_code, chunk_json_data)
            retries += 1
    else:
        logger.exception("Exception during main program execution. The stack trace is as follows:" + traceback.format_exc())
        logger.error("Max retries exceeded. JSON Validation Error or other issue.")


def handle_Wfd_error(response):
    try:
        status_code = response.json()
        error_response_from_Wfd = status_code.get("details", {}).get("results", [])
        unique_error_messages = set()
        unique_error_codes = set()
        unique_error_data_list = []

        for error_result in error_response_from_Wfd:
            error = error_result.get("error", {})
            error_code = error.get("errorCode", "Unknown")
            error_message = error.get("message", "Unknown Error")
            unique_error_codes.add(error_code)
            unique_error_messages.add(error_message)

            error_data_list = error.get("details", {}).get("input", {}).get("orgJob", {}).get("qualifier", None)
            unique_error_data_list.append(error_data_list)

        raise Exception(f"Response From Wfd: {status_code} Status Text is {status_code.get('Status')} Unique Error Codes: {unique_error_codes}, Unique Error Messages: {unique_error_messages}, Unique Error Data List: {unique_error_data_list}")
    except Exception as e:
        raise Exception(f"Error handling Wfd response: {e}")


def handle_retry_exception(exception, retries, store_code, chunk_json_string):
    logger.error(f"Error during Wfd API request: {exception}")

    if retries < 4:
        wait = retries * 40
        logger.error(f"Error! Retry {retries + 1}: Waiting {wait} secs and re-trying...")
        logger.error(f"The JSON API Body is: {chunk_json_string}")
        time.sleep(wait)
    else:
        logger.exception("Exception during main program execution. The stack trace is as follows:" + traceback.format_exc())
        logger.error(f"Max retries exceeded. Writing to S3.")
        filename =f"hours-forecast-earned-wfd/_retry_failed.json"
        upload_to_s3(chunk_json_string,output_bucket,filename)

def authenticate_wfd():
    access_url = f"{hostname}{url_token}"
    
    headers = {
        "appkey": secrets["appkey"],
        "Content-Type": "application/x-www-form-urlencoded"
    }

    # Assuming you need to send some form data for authentication
    data = {
        "username": secrets["username"],
        "password": secrets["password"],
        "grant_type": "password",
        "client_id": secrets["client_id"],
        "client_secret": secrets["client_secret"]
    }
 
    response = requests.post(access_url, headers=headers, data=data)
    if response.status_code == 200:
        access_token = response.json().get("access_token")
        return access_token
    else:
        raise Exception(f"Authentication Error: {response.status_code}, {response.text}")

 
 
def json_validation_deletion_input(json_string, send_only_one_chunk=True, delete_csv_after_validation=True):
 
    json_validation = validate_json(json_string)    
    json_data = json.loads(json_string)
    qualifier = json_data["orgJob"]["qualifier"]
    parts = qualifier.split("/")
    store_code_match = re.search(r'(\d+)-(.+)', '/'.join(parts))
    if store_code_match:
        store_code = "S" + store_code_match.group(1)
    else:
        logger.info("Store Code not found in the qualifier.")
        logger.info("Debug Info:")
        logger.info("Qualifier:", qualifier)
        logger.info("Parts:", parts)
 
    if json_validation:
        try:
            headcounts_per_day_chunks = [json_data["headcountsPerDay"][i:i + 28] for i in range(0, len(json_data["headcountsPerDay"]), 28)]
            if send_only_one_chunk:
                chunk_json_data = {
                    "orgJobs": [
                        {
                            "orgJob": {
                                "qualifier": qualifier
                            },
                            "headcountsPerDay": headcounts_per_day_chunks[0]
                        }
                    ]
                }
                chunk_json_string = json.dumps(chunk_json_data, indent=2)
                access_token=authenticate_wfd()
                print(f"access_token_1:{access_token}")
                post_chunk_to_Wfd(chunk_json_string, store_code,access_token=access_token)
            else:
                for chunk in headcounts_per_day_chunks:
                    chunk_json_data = {
                        "orgJobs": [
                            {
                                "orgJob": {
                                    "qualifier": qualifier
                                },
                                "headcountsPerDay": chunk
                            }
                        ]
                    }
                    chunk_json_string = json.dumps(chunk_json_data, indent=2)
                    access_token=authenticate_wfd()
                    
                    post_chunk_to_Wfd(chunk_json_string, store_code, access_token=access_token)
            if delete_csv_after_validation:
                input_s3_key = f"input/{qualifier.replace('/', '_')}.csv"
                s3.delete_object(Bucket=bucket_name, Key=input_s3_key)
                logger.info(f"Deleted CSV file: {input_s3_key}")
        except Exception as e:
            logger.exception("Exception during main program execution. The stack trace is as follows:" + traceback.format_exc())
            logger.error("Error during chunking or processing: {}".format(e))  
    else:
        logger.error("JSON Validation Error: {}".format(json_string))
        
def process_head_counts(csv_content):
    head_counts = {}
    csv_reader = csv.reader(csv_content.splitlines())
    next(csv_reader)  # Skip header row
    for row in csv_reader:
        store = row[0]
        department = row[1]
        job = row[2]
        date = datetime.strptime(row[3], '%d-%b-%y').date()
        time_slot = datetime.strptime(row[4], '%H:%M').time()
        hours = int(float(row[5]))
        rounded_time_slot = round_time_to_15_minutes(time_slot)
        key = (store, department, job, date)
        if key not in head_counts:
            head_counts[key] = {"hours": [0] * 96, "count": 0}
        index = (rounded_time_slot.hour * 4) + (rounded_time_slot.minute // 15)
        head_counts[key]["hours"][index] += hours
        head_counts[key]["count"] += 1
    return head_counts

def generate_json_structure(store_data, time_slot_hours):
    json_data = {
        "orgJobs": []
    }
    store_data['Date'] = pd.to_datetime(store_data['Date'])
    for index, row in store_data.iterrows():
        if pd.notna(row['Date']):  # Check for NaT before formatting
            query_key = (row['STORE'], row['Department'], row['Job'], row['Date'].date())
            head_count_str = ",".join(map(str, time_slot_hours[query_key]["hours"]))
            json_structure = {
                "orgJob": {
                    "qualifier": f"{row['STORE_PATH']}/{row['Department']}/{row['Job']}"
                },
                "headcountsPerDay": [
                    {
                        "date": row['Date'].strftime('%Y-%m-%d'),
                        "headcounts": head_count_str
                    }
                ],
                "count": time_slot_hours[query_key]["count"]
            }
            json_data["orgJobs"].append(json_structure)
    return json_data

def create_json_file(json_data):
    """
    Creates a JSON file and returns its content as a string.
    """
    return json.dumps(json_data,indent=2)

def download_from_s3(s3_bucket, s3_key):
    """
    Downloads a file from S3 bucket and returns its content as a string.
    """
    try:
        response = s3.get_object(Bucket=s3_bucket, Key=s3_key)
        content = response['Body'].read().decode('utf-8')
        return content
    except Exception as e:
        logger.exception("Exception during main program execution. The stack trace is as follows:" + traceback.format_exc())
        logger.error(f"Error downloading from S3: {e}")
        raise  # Re-raise the exception

def upload_to_s3(content, s3_bucket, s3_key):
    """
    Uploads content to S3 bucket.
    """
    try:
        s3.put_object(Body=content, Bucket=s3_bucket, Key=s3_key)
    except Exception as e:
        logger.exception("Exception during main program execution. The stack trace is as follows:" + traceback.format_exc())
        logger.error(f"Error uploading to S3: {e}")
        raise  # Re-raise the exception

def handler(event, context):
    try:
        logger.info("Start of create store hours Lambda execution")
        json_data_list = []
        lookup_content = download_from_s3(bucket_name, lookup_s3_key)
        df_store_lookup_data = pd.read_csv(StringIO(lookup_content))
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=input_folder)
        for s3_object in response.get('Contents', []):
            s3_key = s3_object['Key']
            file_content = download_from_s3(bucket_name, s3_key)
            if not file_content.strip():
                continue  # Skip to the next iteration
            try:
                df_input_data = pd.read_csv(StringIO(file_content))
            except pd.errors.EmptyDataError:
                print(f"Empty data error: Could not parse CSV content for file {s3_key}")
                continue  # Skip to the next iteration
            df_store_data = pd.merge(df_store_lookup_data, df_input_data, how='left', left_on='STORE', right_on='Store')
            time_slot_hours = process_head_counts(file_content)
            json_data_list.append(generate_json_structure(df_store_data, time_slot_hours))
        grouped_data = process_json_string_list(json_data_list)
        min_date = None
        max_date = None
        for org_job, headcounts_per_day in grouped_data.items():
            for entry in headcounts_per_day:
                date_str = entry['date']
                date = datetime.strptime(date_str, '%Y-%m-%d').date()
                if min_date is None or date < min_date:
                    min_date = date 
                if max_date is None or date > max_date:
                    max_date = date
        min_date = min_date + timedelta(days=(6 - min_date.weekday() + 7) % 7)
        max_date = max_date - timedelta(days=(max_date.weekday() + 2) % 7)
        all_dates = [min_date + timedelta(days=x) for x in range((max_date - min_date).days + 1)]
        for org_job, headcounts_per_day in grouped_data.items():
            date_headcounts_dict = {date: [0] * 96 for date in all_dates}
            for entry in headcounts_per_day:
                date_str = entry['date']
                date = datetime.strptime(date_str, '%Y-%m-%d').date()
                if not (min_date <= date <= max_date):
                    continue
                index = all_dates.index(date)
                headcounts_str = entry['headcounts'].split(',')
                date_headcounts_dict[date] = list(map(int, headcounts_str))
            json_data = {
                "orgJob": {
                    "qualifier": org_job
                },
                "headcountsPerDay": [
                    {
                        "date": date.strftime('%Y-%m-%d'),
                        "headcounts": ",".join(map(str, date_headcounts_dict[date]))
                    }
                    for date in all_dates
                ]
            }
            try:
                json_content = create_json_file(json_data)
                # access_token = authenticate_wfd()
                
                #json_validation_deletion_input(json_content, send_only_one_chunk=False,header = {"accept": "application/json","content-type": "application/json"}, delete_csv_after_validation=True) #all data evry 28 data chunk will be genrated post that the file from input bucket will be deleted
                #json_validation_deletion_input(json_content, send_only_one_chunk=True,header = {"accept": "application/json","content-type": "application/json"}, delete_csv_after_validation=False) #all data first 28 data chunk will be genrated
            except Exception as e:
                logger.exception("Exception during main program execution. The stack trace is as follows:" + traceback.format_exc())
                logger.error(f"Error uploading file to S3: {e}")
        json_validation_deletion_input(json_content, send_only_one_chunk=True,delete_csv_after_validation=False) # can be used for testing last files first 28 data will be counted
        #json_validation_deletion_input(json_content, send_only_one_chunk=False, delete_csv_after_validation=False) # can be used for testing last files all data will be counted            
                    
    except Exception as e:
        logger.exception("Exception during main program execution. The stack trace is as follows:" + traceback.format_exc())
        logger.error(f"An error occurred: {e}")
    finally:
        logger.info("End of create store hours Lambda execution")
        return