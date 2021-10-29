import json
import os
import logging
import azure.functions as func
from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient, ResourceTypes, AccountSasPermissions, generate_account_sas

def main(event: func.EventGridEvent):
    result = json.dumps({
        'id': event.id,
        'data': event.get_json(),
        'topic': event.topic,
        'subject': event.subject,
        'event_type': event.event_type,
        'name': 'Brent McConnell'
    })

    if event.event_type != 'Microsoft.Storage.BlobCreated':
        return     
        
    logging.info('Python EventGrid trigger processed an event: %s', result)
    
    connection_string = os.environ["app_STORAGE"]

    today = datetime.today()
    timestamp = today.timestamp()
    date = today.strftime("%m-%d-%Y")
    path = date + "/" + str(timestamp)
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

    source_container_name = "files"
    target_container_name = "copy"

    source_blob_path = (f"https://{account_name}.blob.core.windows.net/{source_container_name}/webapp.bicep?{sas_token}")
    target_blob_path = (f"https://{account_name}.blob.core.windows.net/{target_container_name}/new_target.bicep")

    source_container_client = blob_service_client.get_container_client(source_container_name)
    target_container_client = blob_service_client.get_container_client(target_container_name)

    source_blob = source_container_client.get_blob_client("webapp.bicep")
    dest_blob = target_container_client.get_blob_client("new_target.bicep")
    dest_blob.start_copy_from_url(source_blob_path, requires_sync=True)
    copy_properties = dest_blob.get_blob_properties().copy

    if copy_properties.status != "success":
        dest_blob.abort_copy(copy_properties.id)
        raise Exception(
            f"Unable to copy blob %s with status %s"
            % (source_blob_path, copy_properties.status)
        )

    #source_blob.delete_blob()



