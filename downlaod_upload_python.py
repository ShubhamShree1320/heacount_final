import json
import time
import os
from collections import defaultdict
from datetime import timedelta
from datetime import datetime
import csv
import re
import pandas as pd
import logging
import boto3
import traceback
from io import StringIO
from botocore.config import Config
import requests

ddb_table_name = "my_db_table"
hostname = "https://sainsburysretail-dev.npr.mykronos.com"
url = "api/v1/forecasting/labor_forecast/multi_create"
secret_id = "my_seecret"
bucket_name = "adapt-dev-export-new"
input_folder = "hours-forecast-earned-wfd"
lookup_s3_key = "store_lookup/Store_Lookup_Table_UKGPro.csv"
output_folder = "headcount-wfd-error-api"
parameter_name = "/dev_wfd_integration/token"
logger = logging.getLogger()
logger.setLevel(logging.INFO)
config = Config(retries={"max_attempts": 10, "mode": "standard"})
s3 = boto3.client("s3", config=config)
dynamodb = boto3.resource("dynamodb", config=config)
s3_resource = boto3.resource(
    "s3",
    config=boto3.session.Config(
        signature_version="s3v4", retries={"max_attempts": 10, "mode": "standard"}
    ),
)
date_iso_format = str(datetime.now().replace(microsecond=0).isoformat())
today_date = str(datetime.now().date())
secretsmanager = boto3.client("secretsmanager", config=config)
response = secretsmanager.get_secret_value(SecretId=secret_id)
secret_string = response["SecretString"]
secret = json.loads(secret_string)
ssm = boto3.client("ssm", config=config)
access_token = "-HWWfUuElrjsw5Ks5n1POWotU_w"
table = dynamodb.Table(ddb_table_name)


def round_time_to_15_minutes(time):
    """
    Round the given time to the nearest 15 minutes.
    :param time: The time to be rounded (datetime.time object)
    :return: The rounded time (datetime.time object)
    """
    datetime_obj = datetime.combine(datetime.today(), time)
    rounded_datetime = datetime_obj + timedelta(
        minutes=(7 + time.minute) // 15 * 15 - time.minute
    )
    return rounded_datetime.time()


def validate_json(json_input):
    """
    Validate JSON string or dictionary.
    :param json_input: JSON in string or dictionary format
    :return: True if valid, False otherwise
    """
    if isinstance(json_input, str):
        try:
            json.loads(json_input)
        except json.JSONDecodeError:
            return False
    elif isinstance(json_input, dict):
        pass
    else:
        return False
    return True


def process_head_counts(csv_content):
    """
    Process head counts from CSV content.
    :param csv_content: String containing CSV data
    :return: Dictionary containing head counts data
    """
    head_counts = {}
    csv_reader = csv.reader(csv_content.splitlines())
    next(csv_reader)
    for row in csv_reader:
        store = row[0]
        department = row[1]
        job = row[2]
        try:
            date = datetime.strptime(row[3], "%d-%b-%y").date()
        except ValueError as e:
            raise ValueError(f"Error parsing date '{row[3]}': {str(e)}")
        time_slot = datetime.strptime(row[4], "%H:%M").time()
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
    """
    Generate JSON structure from store data and time slot hours.
    :param store_data: DataFrame containing store data
    :param time_slot_hours: Dictionary containing aggregated headcounts per time slot
    :return: JSON structure for API request
    """
    json_data = {"orgJobs": []}
    store_data["Date"] = pd.to_datetime(store_data["Date"])
    for index, row in store_data.iterrows():
        if pd.notna(row["Date"]):
            query_key = (
                row["STORE"],
                row["Department"],
                row["Job"],
                row["Date"].date(),
            )
            head_count_str = ",".join(map(str, time_slot_hours[query_key]["hours"]))
            json_structure = {
                "orgJob": {
                    "qualifier": f"{row['STORE_PATH']}/{row['Department']}/{row['Job']}"
                },
                "headcountsPerDay": [
                    {
                        "date": row["Date"].strftime("%Y-%m-%d"),
                        "headcounts": head_count_str,
                    }
                ],
                "count": time_slot_hours[query_key]["count"],
            }
            json_data["orgJobs"].append(json_structure)
    return json_data


def process_json_string_list(json_data_list):
    """
    Process a list of JSON strings containing organizational job data. Group the data by organizational job qualifier.
    :param json_data_list: List of JSON strings containing organizational job data
    :return: Dictionary where keys are organizational job qualifiers and values are lists of headcounts per day
    """
    grouped_data = defaultdict(list)
    for json_data in json_data_list:
        for org_job_entry in json_data.get("orgJobs", []):
            org_job = org_job_entry.get("orgJob", {}).get("qualifier", "")
            headcounts_per_day = org_job_entry.get("headcountsPerDay", [])
            grouped_data[org_job].extend(headcounts_per_day)
    return grouped_data


def handle_retry_exception(exception, retries):
    """
    Handle retry attempts for Wfd API requests. Log the error message and retry information.
    :param exception: The exception raised during the API request
    :param retries: Number of retry attempts made
    :return: None
    """
    logger.error(f"Error during Wfd API request: {exception}")
    if retries < 1:
        wait = retries * 30
        logger.error(
            f"Error! Retry {retries + 1}: Waiting {wait} secs and re-trying..."
        )
        time.sleep(wait)


def post_data_to_wfd_api(json_string, start_date, end_date, store_code, success_count):
    """
    Post JSON chunk data to Workforce Distribution. Retry in case of failure.
    :param json_string: JSON data chunk to post
    :param start_date: Start date of the data chunk
    :param end_date: End date of the data chunk
    :param store_code: Code of the store
    :param success_count: Number of successful attempts
    :return: List of unique error messages encountered during posting
    """
    global access_token
    retries = 0
    max_retries = 1
    response_content = None
    response = None
    unique_error_message = []
    response_content_str = None
    json_data = json.loads(json_string)
    validate = validate_json(json_data)
    if validate:
        while retries < max_retries:
            try:
                api = f"{hostname}/{url}"
                headers = {
                    "Authorization": f"{access_token}",
                    "appkey": secret["appkey"],
                    "Content-Type": "application/json",
                }                
                response = requests.post(
                    api, headers=headers, json=json_data, timeout=45
                )
                #comment the above line and umcomment the below line to mute the api call
                print(f"response.status_code={response.status_code}")
                response_content = response.text
                if response.status_code == 200:
                    logger.info("Labour Forecast created successfully")
                    success_count += 1
                    break
                elif response.status_code == 401:
                    access_token = ssm.get_parameter(Name=parameter_name, WithDecryption=True)["Parameter"]["Value"]
                    continue
                else:
                    raise Exception(
                        f"Respose status = {response.status_code} for JSON in request json_data. Retrying..."
                    )
            except Exception as e:
                handle_retry_exception(e, retries)
                retries += 1
        if retries == max_retries:
            if response_content is not None:
                response_content = json.loads(response_content)
                # Extract error details from the response and handle them
                for result in response_content.get("details", {}).get("results", []):
                    error = result.get("error", {})
                    error_code = error.get("errorCode", "Unknown")
                    error_message = error.get("message", "Unknown Error")
                    # Construct error record message
                    error_record = f"Error in Wfd response for store {store_code}. Error Code: {error_code}, Error Message: {error_message}"
                    # Append unique error messages to the list
                    if error_record not in unique_error_message:
                        unique_error_message.append(error_record)
                filename = f"{output_folder}/{start_date}-to-{end_date}_{store_code}.json"
                response_content_str = json.dumps(json_data, indent=2)
                try:
                    s3.put_object(
                        Body=response_content_str, Bucket=bucket_name, Key=filename
                    )
                except Exception as e:
                    logger.error(f"Error occurred during S3 object upload: {str(e)}")
            else:
                logger.error("No response content received from the API")
        logger.info(f"Response from UKG:{unique_error_message}")
    else:
        logger.info("JSON Validation Error: %s" % json_string)
    return unique_error_message, success_count


def update_ddb_table(errors, success_count, store_code, market):
    """
    Update DynamoDB table with error records and success count for a specific store code.
    :param store_code: Store code.
    :param errors: List of error records.
    :param success_count: Number of successful updates.
    :param Market: Market information.
    :return: None
    """
    try:
        # Iterate through each error record and add to DynamoDB
        response = table.put_item(
            Item={
                "Store": store_code,
                "successfull": success_count,
                "DateTime": datetime.fromtimestamp(time.time()).strftime("%H:%M:%S"),
                "Date": today_date,
                "error_message": errors,
                "Market": market,
                "Timetoexist": round(
                    ((datetime.now() + timedelta(days=60)) - datetime.now()).days
                ),
            }
        )
        logger.info("DynamoDB Put Item Response:", response)
    except Exception as e:
        logger.error("Error occurred while updating DynamoDB:", str(e))


def Create_json_api_body(data, File_name):
    """
    Create JSON API body from input data.
    :param data: Input data.
    :param File_name: Name of the file
    :return: None
    """

    df = pd.json_normalize(data["orgJobs"], "headcountsPerDay", ["orgJob"])
    df["date"] = pd.to_datetime(df["date"])
    min_date = df["date"].min()
    success_count = 0
    (len(df["date"].unique()) - 1) // 28 + 1
    group_size = 28
    df["group_number"] = ((df["date"] - min_date).dt.days // group_size) + 1
    unique_groups = df["group_number"].unique()
    for group_num in unique_groups:
        group_df = df[df["group_number"] == group_num]
        json_records = {"orgJobs": []}
        if "orgJob" not in group_df.columns:
            raise ValueError(
                "'orgJob' column not found in DataFrame. Data format is incorrect."
            )
        group_df.loc[:, "orgJob_qualifier"] = group_df["orgJob"].apply(
            lambda x: x["qualifier"]
        )
        for orgJob_qualifier, orgjob_group in group_df.groupby("orgJob_qualifier"):
            orgjob_record = {
                "orgJob": {"qualifier": orgJob_qualifier},
                "headcountsPerDay": [],
            }
            for index, row in orgjob_group.iterrows():
                date_str = row["date"].strftime("%Y-%m-%d")
                headcount_record = {"date": date_str, "headcounts": row["headcounts"]}
                orgjob_record["headcountsPerDay"].append(headcount_record)
            json_records["orgJobs"].append(orgjob_record)
        min_group_date = group_df["date"].min().strftime("%Y-%m-%d")
        max_group_date = group_df["date"].max().strftime("%Y-%m-%d")
        json_string = json.dumps(json_records, indent=2)
        store_number_match = re.search(r"_(S|A)(\d{4})\.csv$", File_name)
        store_number = store_number_match.group(1) + store_number_match.group(2).zfill(
            4
        )
        market_name = re.search(r"_(Argos|Saisburys)_", File_name)
        market = market_name.group(1)
        response = post_data_to_wfd_api(
            json_string, min_group_date, max_group_date, store_number, success_count
        )
        error_messages, success_count = response
    # Uncomment the below line to delete csv file from the input bucket
    # s3_resource.Object(bucket_name, File_name).delete()
    # logger.info("File {} has been deleted after processing.".format(File_name))
    update_ddb_table(error_messages, success_count, store_number,market)


def process_file(file_key):
    """
    Processes the specified file from an S3 bucket, generates JSON data, and posts it to an API endpoint.
    :param file_key: The key of the file to process.
    :return: None
    """

    try:
        json_data_list = []
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        content = response["Body"].read().decode("utf-8")
        lookup_content = (
            s3.get_object(Bucket=bucket_name, Key=lookup_s3_key)["Body"]
            .read()
            .decode("utf-8")
        )
        df_store_lookup_data = pd.read_csv(StringIO(lookup_content))
        if not content.strip():
            return
        df_input_data = pd.read_csv(StringIO(content))
        df_store_data = pd.merge(
            df_store_lookup_data,
            df_input_data,
            how="left",
            left_on="STORE",
            right_on="Store",
        )
        time_slot_hours = process_head_counts(content)
        json_data_list.append(generate_json_structure(df_store_data, time_slot_hours))
        grouped_data = process_json_string_list(json_data_list)
        min_date = None
        max_date = None
        # Iterate over each organization job and its headcounts per day
        for org_job, headcounts_per_day in grouped_data.items():
            # Iterate over each entry in headcounts_per_day
            for entry in headcounts_per_day:
                date_str = entry["date"]
                date = datetime.strptime(date_str, "%Y-%m-%d").date()
                # Update min_date if current date is earlier
                if min_date is None or date < min_date:
                    min_date = date
                # Update max_date if current date is later
                if max_date is None or date > max_date:
                    max_date = date
        # Adjust min_date to the next Sunday and max_date to the previous Saturday
        min_date = min_date + timedelta(days=(6 - min_date.weekday() + 7) % 7)
        max_date = max_date - timedelta(days=(max_date.weekday() + 2) % 7)
        # Generate a list of all dates between min_date and max_date (inclusive)
        all_dates = [
            min_date + timedelta(days=x) for x in range((max_date - min_date).days + 1)
        ]
        json_data = {"orgJobs": []}
        for org_job, headcounts_per_day in grouped_data.items():
            date_headcounts_dict = {date: [0] * 96 for date in all_dates}
            for entry in headcounts_per_day:
                date_str = entry["date"]
                date = datetime.strptime(date_str, "%Y-%m-%d").date()
                if not (min_date <= date <= max_date):
                    continue
                all_dates.index(date)
                headcounts_str = entry["headcounts"].split(",")
                date_headcounts_dict[date] = list(map(int, headcounts_str))
            headcounts_current_period = [
                {
                    "date": date.strftime("%Y-%m-%d"),
                    "headcounts": ",".join(map(str, date_headcounts_dict[date])),
                }
                for date in all_dates
            ]
            json_data["orgJobs"].append(
                {
                    "orgJob": {"qualifier": org_job},
                    "headcountsPerDay": headcounts_current_period,
                }
            )
        Create_json_api_body(json_data, file_key)
    except Exception as e:
        error_message = f"Error processing file {file_key}: {str(e)}"
        logger.exception(
            f"Exception during program execution. The stack trace is as follows:{error_message}"
            + traceback.format_exc()
        )


if __name__ == "__main__":
    try:
        print("Start of create store hours Lambda execution")
        response = s3.list_objects(Bucket=bucket_name, Prefix=input_folder)
        file_counter = 0
        if "Contents" in response:
            for obj in response["Contents"]:
                file_key = obj["Key"]
                print("Processing file:", file_key)
                process_file(file_key)
                print("File processed successfully:", file_key)
    except Exception as e:
        print("Error occurred:", str(e))
    finally:
        print("End of create store hours Lambda execution")

