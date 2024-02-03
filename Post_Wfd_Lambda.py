import json,time,os
import logging
import boto3
from io import BytesIO, StringIO
from botocore.config import Config
import requests
import traceback
logger = logging.getLogger()
logger.setLevel(logging.INFO)
config = Config(retries={'max_attempts': 10, 'mode': 'standard'})
s3 = boto3.client('s3', config=config) 
secretsmanager = boto3.client('secretsmanager')
s3_resource = boto3.resource('s3', config=boto3.session.Config(signature_version='s3v4', retries = {
            'max_attempts': 10,
            'mode': 'standard'
            }))
secret_id = os.environ["secret_id"]
hostname = os.environ["hostname"]
url=os.environ["url"]
bucket_name=os.environ["input_bucket"]
input_folder=os.environ["input_folder"]

def post_json_to_Wfd_Api(secret_id,file_key,hostname,url):
    try:
        retries = 0
        max_retries = 4
        json_string=None
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        content = response['Body'].read().decode('utf-8')
        json_data = json.loads(content)
        #retrieve accesstoken from secret manager 
        response = secretsmanager.get_secret_value(SecretId=secret_id)
        secret_string = response['SecretString']
        secret = json.loads(secret_string)
        access_token=secret["api_token"]
        api = f"{hostname}{url}"
        headers = {
                    "Authorization": f"{access_token}",
                    "appkey": secret["appkey"],
                    "Content-Type": "application/json"
                }
        while retries<max_retries:
            response = requests.post(api, headers=headers, json=json_data)
            #response_content = response.text
            if response.status_code == 200:
                logger.info("Labor Forecast created successfully")
                s3_resource.Object(bucket_name, file_key).delete()
                break
            else:
                handle_retry_exception(retries)
                retries += 1
        if retries == max_retries:
            s3_resource.Object(bucket_name, file_key).delete()
            # print("save file to resend folder")            
    except Exception as e:
        logger.exception("Exception during program execution.")

def handle_retry_exception(retries):  
    '''
        Handle retry attempts for Wfd API requests. Log the error message and retry information.
        :param exception: The exception raised during the API request
        :param retries: Number of retry attempts made
    '''
    logger.error(f"Error during Wfd API request")
    if retries < 4:
        wait = retries * 40
        logger.error(f"Error! Retry {retries + 1}: Waiting {wait} secs and re-trying...")
        time.sleep(wait)

def handler(event, context):
    try:
        print("Start of create store hours Lambda execution")
        response = s3.list_objects(Bucket=bucket_name, Prefix=input_folder)
        if 'Contents' in response:
            for obj in response['Contents'][1:]:
                file_key = obj['Key']
                remaining_time = context.get_remaining_time_in_millis()
                remaining_time_seconds = remaining_time // 1000 # converts it into seconds
                minutes, seconds = divmod(remaining_time_seconds, 60) # get min and seconds usin div mod
                remaining_time = f"{minutes:02}:{seconds:02}"
                if remaining_time < '02:00':  # Assuming 60 seconds threshold, adjust as needed
                    raise Exception("Insufficient time remaining, stopping further processing.")
                post_json_to_Wfd_Api(secret_id, file_key, hostname, url)
                logger.info(f"{file_key} processed successfully")
    except Exception as e:
        print("Error occurred:", str(e))
    finally:
        print("End of create store hours Lambda execution")