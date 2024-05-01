import pandas as pd
import numpy as np
import json
import requests
import re
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from io import StringIO
import os
from azure.storage.blob import BlobServiceClient
import pandas as pd
from io import BytesIO, StringIO

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


URL = "https://data.cdc.gov/api/views/hfr9-rurv/rows.csv?accessType=DOWNLOAD"

response = requests.get(URL, verify=False)
if response.status_code == 200:
    # Decode the content and read into DataFrame
    df_raw = pd.read_csv(BytesIO(response.content))
    # Print the first few rows to verify if the data has been read successfully
    print(df_raw.head())
    print(df_raw.columns)
    print(df_raw.shape)
    df_raw.info()
else:
    print("Failed to download the file.")

df_cleaned = df_raw.copy()
df_cleaned = df_raw.drop(columns=  ['LocationAbbr','Data_Value_Footnote_Symbol','Data_Value_Footnote','Datasource','Data_Value_Unit','Data_Value_Alt','StratificationCategory1','StratificationCategoryID1','StratificationID1','StratificationCategoryID2','StratificationID2'])
df_cleaned = df_cleaned.dropna(subset=['Data_Value'])
df_cleaned.info()

df_cleaned['Longitude'] = None
df_cleaned['Latitude'] = None

for index, row in df_cleaned.iterrows():
    geolocation = row['Geolocation']
    if geolocation:
        match = re.match(r'POINT \(([-0-9.]+) ([-0-9.]+)\)', geolocation)
        if match:
            longitude = float(match.group(1))
            latitude = float(match.group(2))
            df_cleaned.at[index, 'Longitude'] = longitude
            df_cleaned.at[index, 'Latitude'] = latitude

alzheimer_df = df_cleaned.copy()

alzheimer_df.head(5)

config_file_path = "config.json"

with open(config_file_path, 'r') as config_file:
  config = json.load(config_file)

CONNECTION_STRING_AZURE_STORAGE = config["connectionString"]
CONTAINER_AZURE = 'alzheimer'
blob_name = "alzheimer.csv"
# Convert DataFrame to CSV
output = StringIO()
df_raw.to_csv(output, index=False)
data = output.getvalue()
output.close()

csv_string = alzheimer_df.to_csv(index=False)

# Initialize BlobServiceClient
blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING_AZURE_STORAGE)

# Get the container client
container_client = blob_service_client.get_container_client(CONTAINER_AZURE)

# Upload the CSV string to Azure Blob Storage
# add raw data code
blob_client = container_client.get_blob_client(blob="alzheimer_data.csv")

blob_client.upload_blob(csv_string)

print(f"Uploaded {blob_name} to Azure Blob Storage in container {CONTAINER_AZURE}.")