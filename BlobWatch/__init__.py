import azure.functions as func
import os
import logging
import azure.functions as func
from pathlib import Path
from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient, ResourceTypes, AccountSasPermissions, generate_account_sas

# Method that returns a tuple of a filepath without the starting "/"
def file_tuple(path):
    path_parts = Path(path).parts
    return path_parts[1:] if path_parts[0] == "/" else path_parts

# This method returns a tuple that does not include the other tuple elements
# in order.  For instance ('hi','there') vs ('there', 'hi') returns ()
# ('hi','there','world') vs ('hi','there') returns ('world',).  Hope that makes sense.
def subtract_common_path_elements(path1_parts, path2_parts):
    num_common_elements=0
    for x in range(min(len(path1_parts),len(path2_parts))):
        if path1_parts[x] == path2_parts[x]:
            num_common_elements = x + 1

    if len(path1_parts) == len(path2_parts):
        return ()

    if len(path1_parts) < len(path2_parts):
        return path2_parts[num_common_elements:]
    else:
        return path1_parts[num_common_elements:]

# Method to return a path using a tuple of strings
# join will insert / between characters of string if 
# only one string is in the tuple thus the if stmt
def construct_file_path(path_tuple):
    if len(path_tuple) == 1:
        return path_tuple[0]
    return "/".join(path_tuple)

# Method to copy blob from input_PATH to output_PATH
# You should ensure that function.json is setup to correctly
# fire on the input_PATH
def main(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob \n"
                 f"Name: {myblob.name}\n"
                 f"Blob Size: {myblob.length} bytes")

    # Grab today's date in mm-dd-YY
    today = datetime.today()
    date = today.strftime("%Y-%m-%d")

    # Retrieve Settings
    input_path = os.environ["input_PATH"]
    output_path = os.environ["output_PATH"]

    # Input_path should be a subset of the blob name or something is wrong
    if not myblob.name.startswith(input_path.lstrip("/")):
        raise Exception(f"Function fired but does not match input_PATH.  Ensure input_PATH variable and functions.json are set correctly")
    
    myblob_tuple = file_tuple(myblob.name)

    # Get container info from settings
    input_tuple = file_tuple(input_path) 
    output_tuple = file_tuple(output_path) 
    if len(output_tuple)==0 or len(input_tuple)==0:
        raise Exception(f"Ensure output_PATH and input_PATH variables are set correctly.  Must at least have a container in the path")


    # At this point we know there is at least 1 element in the tuple and it should be the container
    # although at this point the output container has not been tested to ensure it exists
    input_container_name = input_tuple[0]
    output_container_name = output_tuple[0]

    # Subtract the input_path from the blob name leaving the filename
    filepath_tuple = subtract_common_path_elements(myblob_tuple, input_tuple)
    if len(filepath_tuple) == 0:
        raise Exception(f"This appears to be a misconfiguration. Ensure input_PATH is set correctly")

    # Strip off the container name from the paths.  We are creating a tuple to pass to the function.
    output_path_tuple = subtract_common_path_elements(output_tuple, (output_container_name,))
    myblob_minus_container_tuple = subtract_common_path_elements(myblob_tuple, (input_container_name,))

    # This is the filepath only of the incoming file with the input_PATH removed
    filepath_only = construct_file_path(filepath_tuple)
    
    # This is the output path with the container removed since we don't use the containername in the
    # copy command below since it is implicit in the container client 
    output_path_only = construct_file_path(output_path_tuple)

    # Just the blob name without the container name
    myblob_without_container_path = construct_file_path(myblob_minus_container_tuple)

    # Construct the path we want the output file to be named but without any container info
    full_output_path = f"{output_path_only}/{date}/{filepath_only}".lstrip("/")

    # Let's log some stuff here
    logging.info(f"filepath_tuple: {filepath_tuple}")
    logging.info(f"output_path_tuple: {output_path_tuple}")
    logging.info(f"myblob_without_container_path: {myblob_without_container_path}")
    logging.info(f"myblob_minus_container_tuple: {myblob_minus_container_tuple}")
    logging.info(f"input_parts: {input_tuple}")
    logging.info(f"output_parts: {output_tuple}")
    logging.info(f"myblob_parts: {myblob_tuple}")
    logging.info(f"output_path_tuple: {output_path_tuple}")
    logging.info(f"filename_only: {filepath_only}")
    logging.info(f"input_container: {input_container_name}")
    logging.info(f"output_container: {output_container_name}")
    logging.info(f"output_path_only: {output_path_only}")
    logging.info(f"filepath_only: {filepath_only}")
    logging.info(f"myblob_without_container_path: {myblob_without_container_path}")
    logging.info(f"full_output_path: {full_output_path}")

    # Create the BlobServiceClient from the connectionstring in the app settings    
    blob_service_client = BlobServiceClient.from_connection_string(os.environ["app_STORAGE"])

    # Requires a SAS token for copying between containers
    # This is hard coded to be valid for 1 hour below
    sas_token = generate_account_sas(
        blob_service_client.account_name,
        account_key=blob_service_client.credential.account_key,
        resource_types=ResourceTypes(object=True),
        permission=AccountSasPermissions(read=True),
        expiry=datetime.utcnow() + timedelta(hours=1)
    )

    # Get Account Name from client
    account_name = blob_service_client.account_name

    # This sets up the full URL for the blob since other sources can be used besides Azure
    source_blob_path = (f"https://{account_name}.blob.core.windows.net/{myblob.name}?{sas_token}")

    # Create the container clients. source_container is only used if you delete after copy
    source_container_client = blob_service_client.get_container_client(input_container_name)
    target_container_client = blob_service_client.get_container_client(output_container_name)
    
    logging.info(f"source_blob_path: {source_blob_path}")

    # This is used to delete the blob after copy if uncommnented below
    source_blob = source_container_client.get_blob_client(myblob_without_container_path)

    # Get the reference to the blob we are creating
    dest_blob = target_container_client.get_blob_client(f"{full_output_path}")

    # Start the copy operation
    dest_blob.start_copy_from_url(source_blob_path, requires_sync=True)

    # Check on the status of the copy and abort if required
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
