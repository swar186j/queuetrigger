import subprocess
import sys
import time
import logging
import json
import os
from urllib.parse import unquote


logging.info("Process started------")
# os.system("pip install requests")
# # os.system("pip install azure-functions")
# os.system("pip install azure-storage-blob")

# def install():
#     subprocess.check_call([sys.executable, "-m", "pip", "install","-r", "requirements.txt"])
#     logging.info("Successfully installed requirements module")
# install()
# def install(package):
#     # This function will install a package if it is not present
#     from importlib import import_module
#     try:
#         import_module(package)
#     except:
#         from sys import executable as se
#         from subprocess import check_call
#         check_call([se,'-m','pip','-q','install',package])
#         logging.info("Successfully installed python modules")
        


# for package in ['requests','azure-functions','azure-storage','azure-storage-blob']:
#     install(package)

        
import requests
import azure.functions as func
from azure.storage.blob import BlobServiceClient,BlobLeaseClient

app = func.FunctionApp()

@app.queue_trigger(arg_name="azqueue", queue_name="test-nsg-queue-1",
                               connection="AzureWebJobsStorage") 
def queue_trigger(azqueue: func.QueueMessage):
    logging.info('Python Queue trigger processed a message: %s',
                azqueue.get_body().decode('utf-8'))
    # Parse the message to get blob information
    message = azqueue.get_body().decode('utf-8')
    logging.info(f"message received {message}")
    blob_info = json.loads(message)
    
    # Extract container and blob names from URI
    try:
        uri_parts = blob_info['data']['url'].split('/')
        container_name = uri_parts[3]
        blob_name = '/'.join(uri_parts[4:])
        decoded_blob_name = unquote(blob_name)  # Decode the blob name to handle URL-encoded characters

        logging.info(f"Container name: {container_name}")
        logging.info(f"Blob name: {decoded_blob_name}")
    except Exception as e:
        logging.info(f"exception: {e}")

    # Create the BlobServiceClient
    blob_service_client = BlobServiceClient.from_connection_string(os.getenv("AzureWebJobsStorage"))

    # Get the lock blob client
    lock_blob_client = blob_service_client.get_blob_client(container=container_name, blob=decoded_blob_name)
    
    # Retry until the lock is acquired
    while True:
        try:
            # Try to acquire a lease (lock) on the blob
            lease = BlobLeaseClient(lock_blob_client)
            lease.acquire(lease_duration=-1)  # Infinite lease
            logging.info('Lock acquired')
            break
        except Exception as e:
            logging.info('Lock not acquired, retrying...')
            time.sleep(5)  # Wait before retrying
    try:
        # Get the BlobClient
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=decoded_blob_name)

        # Read the checkpoint (if exists) from blob metadata
        properties = blob_client.get_blob_properties()
        metadata = properties.metadata

        logging.info(f"metadat >>>> {metadata}")
        # start_index = int(metadata.get('start_index',0))

        # Read the JSON blob
        blob_data = blob_client.download_blob().readall()
        logs = json.loads(blob_data)['records']
        logging.info('Blob data: %s', len(logs))
        logging.info(f'Blob logs {logs}')

        if start_index:
            logging.info('start index exists: %s', start_index)
            logs = logs[start_index:]
            start_index = len(logs)
        else:
            logging.info('start index not exists: %s', start_index)
            start_index = len(logs)

        for log in logs:
            payload = {"event": log}

            headers = {
                'Authorization': "Bearer af451439de434bee9de58f6a23f7ffef",
                'Content-Type': "application/json"
                }

            res = requests.post("https://ingest.us-1.crowdstrike.com/api/ingest/hec/be05febd4f9f49809f094cec369f92c7/v1/services/collector", json=payload, headers=headers)

            data = res.text
            logging.info(f"data >>>>> {data}")

    finally:
        # Release the lock
        lease.release()
        logging.info('Lock released')

        # Update the checkpoint in metadata
        metadata["start_index"] = str(start_index)
        blob_client.set_blob_metadata(metadata)
        logging.info(f"set blob >>>> {blob_client.set_blob_metadata(metadata)}")

        #verify metadata is updated
        updated_properties = blob_client.get_blob_properties()
        updated_metadata = updated_properties.metadata
        logging.info(f"updated metadata >>>> {updated_metadata}")
