import azure.functions as func
import os
import boto3
import pymongo
from minio import Minio
from pymongo import MongoClient
import datetime
import json
import bson
import logging
from azure.storage.blob import BlobServiceClient
from datetime import datetime, timedelta, timezone
import pytz
from bson import ObjectId, Binary
from botocore.exceptions import NoCredentialsError
import io
import base64
from minio.error import S3Error
from bson import json_util
from botocore.client import Config

app = func.FunctionApp()

@app.timer_trigger(schedule="0 */5 * * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False) 
def func_data_migration(myTimer: func.TimerRequest) -> None:
    
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function executed.')
def transfer_json_data(mongo_client):
    try:
        # Mongodb clienty setup
        db = mongo_client['']
        collection = db['']

        #calculate the date 7 days ago from today
        seven_days_ago =  (datetime.now() - timedelta(days=7))

        # Query MongoDB for documents created in the last 7 days
        query = {"createDateTime": {"$lt": seven_days_ago}}

        # Fetch results
        # Fetch documents that match the query
        documents = list(collection.find(query))
        class CustomJSONEncoder(json.JSONEncoder):
            def default(self, obj):
                if isinstance(obj, datetime):
                    return obj.isoformat()
                if isinstance(obj, ObjectId):
                    return str(obj)
                if isinstance(obj, Binary):
                    return obj.hex()  # or obj.hex() for hex representation
                return super().default(obj)
        # Conver to  JSON string
        json_string = json.dumps(documents, indent=4, cls=CustomJSONEncoder)
        
        # Wasabi S3 client setup (using boto3)
        wasabi_client = boto3.client(
           's3',
          endpoint_url='https://s3.ap-southeast-1.wasabisys.com',
          aws_access_key_id='',
          aws_secret_access_key='',
        )
        # Define Wasabi bucket name
        BUCKET_NAME = 'mongodbdata'
        Date = str(seven_days_ago)
        json_name = Date.split(" ")[0]
        JSON_OBJECT_KEY = f"{json_name}.json"
        print(JSON_OBJECT_KEY)

        # Upload JSON data to Wasabi
        print(f"Uploading data to Wasabi bucket '{BUCKET_NAME}'...")
        wasabi_client.put_object(
                Bucket=BUCKET_NAME,
                Key=JSON_OBJECT_KEY,
                Body=json_string,
                ContentType='application/json'
            )
        print(f"Successfully uploaded {JSON_OBJECT_KEY} to bucket {BUCKET_NAME}.")
    except NoCredentialsError:
        print("Credentials not available")
    except Exception as e:
        print(f"Error: {e}")
    # Delete MongoDB Data
    print("Deleting data from MongoDB...")
    collection.delete_many(query)
    print("Successfully deleted data from MongoDB.")
transfer_json_data(
    mongo_client=pymongo.MongoClient('mongodb')
)
def fetch_blobs_from_azure_and_upload_to_wasabi(
    azure_connection_string, azure_container_name, wasabi_client, wasabi_bucket
):
    try:
        # Initialize Azure Blob Service Client
        blob_service_client = BlobServiceClient.from_connection_string(azure_connection_string)
        container_client = blob_service_client.get_container_client(azure_container_name)

        # Calculate the cutoff date (7 days ago)
        cutoff_date = (datetime.now() - timedelta(days=2))
        print("cutoff_date : ",cutoff_date)
        # List and filter blobs modified in the last 15 days
        print(f"Fetching blobs modified since {cutoff_date}...")
        for blob in container_client.list_blobs():
            print("blob.last_modified",blob.last_modified)

            if blob.last_modified.replace(tzinfo=None) <= cutoff_date:
                blob_name = blob.name
                print(f"Processing blob: {blob_name}, Last Modified: {blob.last_modified}")

                # Download blob content
                blob_client = container_client.get_blob_client(blob_name)
                download_stream = blob_client.download_blob()
                blob_data = download_stream.readall()

                # Upload the blob to Wasabi
                wasabi_client.put_object(
                    Bucket=wasabi_bucket,
                    Key=blob_name,
                    Body=blob_data,
                )
                print(f"Uploaded {blob_name} to Wasabi bucket {wasabi_bucket}")
                # Delete the blob from Azure after successful upload
                blob_client.delete_blob(blob_name)
                print(f"Deleted {blob_name} from Azure container {azure_container_name}")

    except NoCredentialsError:
        print("Wasabi credentials not available")
    except Exception as e:
        print(f"Error: {e}")
# Azure Storage Account Details
AZURE_CONNECTION_STRING = "Azure string"
AZURE_CONTAINER_NAME = "container name"

# Wasabi Details
WASABI_CLIENT = boto3.client(
    's3',
    endpoint_url="https://s3.ap-southeast-1.wasabisys.com",
    aws_access_key_id="key",
    aws_secret_access_key="key"
)
WASABI_BUCKET = "minioimages"

# Fetch and upload blobs
fetch_blobs_from_azure_and_upload_to_wasabi(
    AZURE_CONNECTION_STRING, AZURE_CONTAINER_NAME, WASABI_CLIENT, WASABI_BUCKET
)

