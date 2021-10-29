import azure.functions as func
import os
import logging
import azure.functions as func
from pathlib import Path
from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient, ResourceTypes, AccountSasPermissions, generate_account_sas

def main(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob \n"
                 f"Name: {myblob.name}\n"
                 f"Blob Size: {myblob.length} bytes")

    logging.info(myblob.__dict__)

    # Grab today's date in mm-dd-YY
    today = datetime.today()
    date = today.strftime("%m-%d-%Y")

    # Retrieve Settings
    input_path = os.environ["input_PATH"]
    output_path = os.environ["output_PATH"]
    connection_string = os.environ["app_STORAGE"]

    # Get container info from settings
    input_container_name = Path(input_path).parts[0]
    output_container_name = Path(output_path).parts[0]


    if myblob.name.startswith(input_path):
        filename_only = myblob.name.replace(input_path, '') 
        blob_name = myblob.name.replace(input_container_name + "/", '')
    else:
        raise Exception(f"This should not happen for: {myblob.name}")

    logging.info(f"filename_only: {filename_only}")
    logging.info(f"input_container: {input_container_name}")
    logging.info(f"output_container: {output_container_name}")

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    sas_token = generate_account_sas(
        blob_service_client.account_name,
        account_key=blob_service_client.credential.account_key,
        resource_types=ResourceTypes(object=True),
        permission=AccountSasPermissions(read=True),
        expiry=datetime.utcnow() + timedelta(hours=1)
    )

    # Get Account Name from client
    account_name = blob_service_client.account_name

    source_blob_path = (f"https://{account_name}.blob.core.windows.net/{myblob.name}?{sas_token}")
    target_blob_path = (f"https://{account_name}.blob.core.windows.net/{output_container_name}/{date}/{myblob.name}")

    source_container_client = blob_service_client.get_container_client(input_container_name)
    target_container_client = blob_service_client.get_container_client(output_container_name)
    
    logging.info(f"source_blob_path: {source_blob_path}")
    logging.info(f"target_blob_path: {target_blob_path}")

    source_blob = source_container_client.get_blob_client(blob_name)
    dest_blob = target_container_client.get_blob_client(f"{date}/{filename_only}")
    dest_blob.start_copy_from_url(source_blob_path, requires_sync=True)
    copy_properties = dest_blob.get_blob_properties().copy

    if copy_properties.status != "success":
        dest_blob.abort_copy(copy_properties.id)
        raise Exception(
            f"Unable to copy blob %s with status %s"
            % (source_blob_path, copy_properties.status)
        )

    # Uncomment below to delete incoming blobs from input area
    # Just be aware that they will be deleted immediately and 
    # you may not even know they were uploaded.
    #source_blob.delete_blob()

