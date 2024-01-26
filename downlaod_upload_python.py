import json
import os
import time
from collections import defaultdict
from datetime import timedelta, datetime,date
import csv
import re
import pandas as pd
import logging
import boto3
from io import BytesIO, StringIO
from botocore.config import Config
import requests
# Set up logging to a file
log_filename = "debug_log.txt"
log_filename1="info.txt"
logging.basicConfig(filename=log_filename, level=logging.DEBUG)
logging.basicConfig(filename=log_filename1, level=logging.INFO)
# Initialize the logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)
# Add these variables at the beginning of your script
output_file_path = "C:/Users/Lenovo/Desktop/headount/file/output/error_log.csv"
csv_headers = ["Orgpath", "part", "success/failure", "retry_count", "error_code", "error_message"]
# Add this function to write the information to a CSV file
def write_to_csv(orgpath, part, status, retries, error_code, error_message):
    with open(output_file_path, mode='a', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow([orgpath, part, status, retries, error_code, error_message])
json_directory="C:/Users/Lenovo/Desktop/headount/file/output"
log_file_path = os.path.join(json_directory, log_filename)
bucket_name="head-count"
json_directory="C:/Users/Lenovo/Desktop/headount/file/output"
input_folder = "input"
lookup_s3_key = "lookup/Store_Lookup_Table_UKGPro.csv"
hostname= "https://sainsburysretail-dev.npr.mykronos.com"
url="/api/v1/forecasting/labor_forecast/multi_create/"
# Set Retries for Boto3
config = Config(retries={'max_attempts': 10, 'mode': 'standard'})

# Create S3 Connection
s3 = boto3.client('s3', config=config)

def extract_parts_from_json(json_data):
    org_job_data = json_data.get("orgJobs", [{}])[0].get("orgJob", {})
    qualifier = org_job_data.get("qualifier", "")
    # Split the qualifier string by "/"
    parts = qualifier.split("/")
   # print(parts)

    # # Look for parts that match the pattern '000X-Stratford' and extract the digits
    store_code_match = re.search(r'(\d+)-(.+)', '/'.join(parts))
    dates=json_data.get("orgJobs", [{}])[0].get("headcountsPerDay", {})
    strt_date = dates[0].get("date", "")
    end_date = dates[-1].get("date", "")
    if store_code_match:
        store_code = "S" + store_code_match.group(1)
        location_code_name = store_code_match.group(2).split('/')[0]
        result = {
            "store_code": store_code,
            "market": parts[2],
            "suffix":str(parts[5])+"-"+str(parts[6]),
            "start_date": strt_date,
            "end_date": end_date    
        }
        return result
    else:
        print("Store Code not found in the qualifier.")
        print("Debug Info:")
        print("Qualifier:", qualifier)
        print("Parts:", parts)
        return None


def authenticate_Wfd():
    url = "https://sainsburysretail-dev.npr.mykronos.com/api/authentication/access_token"
    headers = {
        "appkey": "AZbcoqIUAwE5ahMAPRyLDSwpul4OGeug",
        "Content-Type": "application/x-www-form-urlencoded"
    }

    # Assuming you need to send some form data for authentication
    data = {
        "username": "Adapt_Integration",
        "password": "Integration4Adapt@890",
        "grant_type": "password",
        "client_id": "XFrGQlZf5VYzKEuFxoLk8niOUo87c0X3",
        "client_secret": "YIG5O3SbvJnwHk3M"
    }

    response = requests.post(url, headers=headers, data=data)

    if response.status_code == 200:
        access_token = response.json().get("access_token")
        return access_token
    else:
        raise Exception(f"Authentication Error: {response.status_code}, {response.text}")
    

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

def post_chunk_to_Wfd(chunk_json_data, store_code, access_token=None):
    retries = 0
    max_retries = 1
    chunk_json_string = None
    response_content = None  # Added variable to store response content
    response = None  # Initialize the variable outside the try block
    chunk_json_data = json.loads(chunk_json_data)
    data = extract_parts_from_json(chunk_json_data)
    market = data['market']
    suffix=data["suffix"]
    start_date=data["start_date"]
    end_date=data["end_date"]
    validate = validate_json(chunk_json_data)
    while retries < max_retries:
        try:
            api = f"{hostname}{url}"
            headers = {
                "Authorization": f"{access_token}",
                "appkey": "AZbcoqIUAwE5ahMAPRyLDSwpul4OGeug",
                "Content-Type": "application/json"
            }
            response = requests.post(api, headers=headers, json=chunk_json_data)
            response_content=response.text

            # print(f"API Response Content: {response_content}")  # Add this line
            if response.status_code == 200:
                logger.info("Labor Forecast created successfully")
                break
            elif response.status_code == 207:
                logger.info("Partial success in creating labor forecasts")
                break
            elif response.status_code == 401:
                access_token=authenticate_Wfd()
                chunk_json_string = json.dumps(chunk_json_data)
                raise Exception(f"Invalid JSON in the request. Retrying...")
            else:
                chunk_json_string = json.dumps(chunk_json_data)
                raise Exception(f"Invalid JSON in the request. Retrying...")

        except Exception as e:
            handle_retry_exception(e, retries, chunk_json_string)
            retries += 1  # Move the increment here

    if retries == max_retries:
        unique_error_message = []
        unique_error_data_list = []
        unique_error_code_list = []
        response_content = json.loads(response_content)    
        for result in response_content.get("details", {}).get("results", []):
            error = result.get("error", {})
            error_code = error.get("errorCode", "Unknown")
            error_message = error.get("message", "Unknown Error")
            unique_error_message.append(error_message)
            error_data_list = error.get("details", {}).get("input", {}).get("orgJob", {}).get("qualifier", None)
            if error_data_list not in unique_error_data_list:
                unique_error_data_list.append(error_data_list)
            if error_code not in unique_error_code_list:
                unique_error_code_list.append(error_code)
        error_msg = f"Error in Wfd response for store {store_code}. Error Code: {error_code}, Error Message: {error_message}"
        print(error_msg)
        print(f"Max retries exceeded: Writing JSON response to S3 for store {store_code}_{suffix}")
        filename = f"output/{start_date}-to-{end_date}_{store_code}_{market}_{suffix}.json"
        response_content_str = json.dumps(response_content, indent=2)  # Convert dict to string
        status_code = upload_to_s3(response_content_str, bucket_name, filename)  # Upload string instead of dict
        print(f"S3 Upload Status Code: {status_code}")  # Add this line
        if status_code == 200:
            print("File uploaded successfully")
        else:
            print("File upload failed")
            
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
  
def handle_retry_exception(exception, retries,chunk_json_string):
    logger.error(f"Error during Wfd API request: {exception}")
    if retries < 1:
        wait = retries * 40
        logger.error(f"Error! Retry {retries + 1}: Waiting {wait} secs and re-trying...")
        logger.error(f"The JSON API Body is: {chunk_json_string}")
        time.sleep(wait)


def json_validation_deletion_input(json_string, send_only_one_chunk=True, access_token=None):
    success_paths = []  # List to track successfully created paths
    failure_paths = []  # List to track paths that failed to be created
    json_validation = validate_json(json_string)
    json_data = json.loads(json_string)
    qualifier = json_data["orgJob"]["qualifier"]
    parts = qualifier.split("/")
    store_code_match = re.search(r'(\d+)-(.+)', '/'.join(parts))
    if store_code_match:
        store_code = "S" + store_code_match.group(1)
    else:
        print("Store Code not found in the qualifier.")
        print("Debug Info:")
        print("Qualifier: %s" % qualifier)
        print("Parts: %s" % parts)
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
                post_chunk_to_Wfd(chunk_json_string, store_code, access_token)
            else:
                for chunk_index, chunk in enumerate(headcounts_per_day_chunks):
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
                    status = post_chunk_to_Wfd(chunk_json_string, store_code, access_token)
                    if status:
                        success_paths.append(qualifier)
                        print(f"Part {chunk_index + 1} successfully posted for {qualifier}")
                    else:
                        failure_paths.append(qualifier)
                        print(f"Part {chunk_index + 1} failed to post for {qualifier}")
        except Exception as e:
            print("Error during chunking or processing: %s" % e)
    else:
        print("JSON Validation Error: %s" % json_string)

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
        logger.error(f"Error downloading from S3: {e}")
        raise  # Re-raise the exception

def upload_to_s3(content, s3_bucket, s3_key):
    """
    Uploads content to S3 bucket.
    """
    try:
        s3.put_object(Body=content, Bucket=s3_bucket, Key=s3_key)
        return 200  # 200 OK for successful upload
    except Exception as e:
        logger.error(f"Error uploading to S3: {e}")
        return 500  # 500 Internal Server Error for upload failure

if __name__ == "__main__":
    try:

        print("Start of create store hours Lambda execution")

        access_token=authenticate_Wfd()
        json_data_list = []
        lookup_content = download_from_s3(bucket_name, lookup_s3_key)
        df_store_lookup_data = pd.read_csv(StringIO(lookup_content))
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=input_folder)
        for s3_object in response.get('Contents', []):
            s3_key = s3_object['Key']
            file_content = download_from_s3(bucket_name, s3_key)
            if not file_content.strip():
                logger.error(f"Empty file: {s3_key}")
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
                # Save the JSON content to a file with org job as the filename
                json_validation_deletion_input(json_content, send_only_one_chunk=False,access_token=access_token)
                s3.delete_object(Bucket=bucket_name, Key=s3_key)
                print(f"Deleted CSV file: {s3_key}")
            except Exception as e:
                logger.error(f"Error uploading file to S3: {e}")
        #json_validation_deletion_input(json_content, send_only_one_chunk=False,access_token=access_token,delete_csv_after_validation=False) # can be used for testing last files first 28 data will be counted
    except Exception as e:
        logger.error(f"An error occurred: {e}")
         # Add a print statement
        print(f"An error occurred: {e}")
    finally:
        print("End of create store hours Lambda execution")

# Add a print statement
print(f"Debug log file created at: {log_filename}")
