# Azure Functions
def azure_upload_blob(connect_str, container_name, blob_name, data):
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    blob_client.upload_blob(data, overwrite=True)
    print(f"Uploaded to Azure Blob: {blob_name}")

def azure_download_blob(connect_str, container_name, blob_name):
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    download_stream = blob_client.download_blob()
    return download_stream.readall()