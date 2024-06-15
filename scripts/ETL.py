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
import sqlalchemy
from sqlalchemy import create_engine
import pyodbc




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
df_cleaned = df_raw.drop(columns=  ['LocationAbbr','Data_Value_Footnote_Symbol','Data_Value_Footnote','Datasource','Data_Value_Unit','Data_Value_Alt','StratificationCategory1','StratificationCategoryID1'])
df_cleaned = df_cleaned.dropna(subset=['Data_Value'])
df_cleaned.info()


df_cleaned['Longitude'] = None
df_cleaned['Latitude'] = None

for index, row in df_cleaned.iterrows():
    geolocation = row['Geolocation']
    if isinstance(geolocation, float):
        continue  # Skip float values
    if geolocation:
        match = re.match(r'POINT \(([-0-9.]+) ([-0-9.]+)\)', geolocation)
        if match:
            longitude = float(match.group(1))
            latitude = float(match.group(2))
            df_cleaned.at[index, 'Longitude'] = longitude
            df_cleaned.at[index, 'Latitude'] = latitude

alzheimer_df = df_cleaned.copy()

alzheimer_df.head(5)





#connecting azure
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





#creating topic dimention
topic_df = df_cleaned[['TopicID', 'Topic']]

# Dropping duplicates to get unique QuestionID pairs with questions
unique_topic_df = topic_df.drop_duplicates()

# Mapping dictionary
question_mapping = dict(zip(unique_topic_df['TopicID'], unique_topic_df['Topic']))

# Applying the mapping to create a new column with descriptions
df_cleaned['Topic'] = df_cleaned['TopicID'].map(question_mapping)
unique_topic_df = unique_topic_df.rename(columns={'TopicID': 'topic_id', 'Topic': 'topic_desc'})
unique_topic_df





#creating class dimention
class_df = df_cleaned[['ClassID', 'Class']]

# Dropping duplicates to get unique QuestionID pairs with questions
unique_class_df = class_df.drop_duplicates()

# Mapping dictionary
class_mapping = dict(zip(unique_class_df['ClassID'], unique_class_df['Class']))

# Applying the mapping to create a new column with descriptions
df_cleaned['Class'] = df_cleaned['ClassID'].map(class_mapping)
unique_class_df = unique_class_df.rename(columns={'ClassID': 'class_id', 'Class': 'class_desc'})
unique_class_df




#creating question dimention
question_df = df_cleaned[['QuestionID', 'Question']]

# Dropping duplicates to get unique QuestionID pairs with questions
unique_questions_df = question_df.drop_duplicates()

# Mapping dictionary
question_mapping = dict(zip(unique_questions_df['QuestionID'], unique_questions_df['Question']))

# Applying the mapping to create a new column with descriptions
df_cleaned['question_desc'] = df_cleaned['QuestionID'].map(question_mapping)
unique_questions_df = unique_questions_df.rename(columns={'QuestionID': 'question_id', 'Question': 'question_desc'})
unique_questions_df





#creating location dimention
location_lookup_df = pd.read_csv('/Users/lisa/CIS9440HW/docs/filtered_location.csv')
location_lookup_df = location_lookup_df.fillna(0)
new_order = ['location_id', 'location_desc','latitude','longitude']
location_lookup_df = location_lookup_df[new_order]
location_lookup_df.head()






# Creating Year Dimension

# Mapping dictionary
year_mapping = {
    2015: 2015,
    2016: 2016,
    2017: 2017,
    2018: 2018,
    2019: 2019,
    2020: 2020,
    2021: 2021,
    2022: 2022
}

unique_syear_ids = df_cleaned['YearStart'].unique()
# Converting the array of unique values into a DataFrame
unique_syear_df = pd.DataFrame(unique_syear_ids, columns=['year_id'])

# Applying the mapping to create a new column with descriptions
unique_syear_df['year'] = unique_syear_df['year_id'].map(year_mapping)
unique_syear_df = unique_syear_df[unique_syear_df['year_id'] != '<NA>']
unique_syear_df





#Creating stratification dimention
stratification_category2_df = df_cleaned[['StratificationCategoryID2', 'StratificationCategory2']]

# Dropping duplicates to get unique QuestionID pairs with questions
unique_stratification_category2_df = stratification_category2_df.drop_duplicates()

# Mapping dictionary
stratification_category2_mapping = dict(zip(unique_stratification_category2_df['StratificationCategoryID2'], unique_stratification_category2_df['StratificationCategory2']))

# Applying the mapping to create a new column with descriptions
df_cleaned['stratification_category2'] = df_cleaned['StratificationCategoryID2'].map(stratification_category2_mapping)
unique_stratification_category2_df = unique_stratification_category2_df.rename(columns={'StratificationCategoryID2': 'stratification_categoryid2', 'StratificationCategory2': 'stratification_category2'})
unique_stratification_category2_df




stratification2_df = df_cleaned[['StratificationID2', 'Stratification2']]

# Dropping duplicates to get unique QuestionID pairs with questions
unique_stratification2_df = stratification2_df.drop_duplicates()

# Mapping dictionary
stratification2_mapping = dict(zip(unique_stratification2_df['StratificationID2'], unique_stratification2_df['Stratification2']))

# Applying the mapping to create a new column with descriptions
df_cleaned['stratification2'] = df_cleaned['StratificationID2'].map(stratification2_mapping)
unique_stratification2_df = unique_stratification2_df.rename(columns={'StratificationID2': 'stratification2_id', 'Stratification2': 'stratification2'})
unique_stratification2_df



stratification1_df = df_cleaned[['StratificationID1', 'Stratification1']]

# Dropping duplicates to get unique QuestionID pairs with questions
unique_stratification1_df = stratification1_df.drop_duplicates()

# Mapping dictionary
stratification1_mapping = dict(zip(unique_stratification1_df['StratificationID1'], unique_stratification1_df['Stratification1']))

# Applying the mapping to create a new column with descriptions
df_cleaned['stratification1'] = df_cleaned['StratificationID1'].map(stratification1_mapping)
unique_stratification1_df = unique_stratification1_df.rename(columns={'StratificationID1': 'stratification1_id', 'Stratification1': 'stratification1'})
unique_stratification1_df




#Creating percentage fact table
percent_df = df_cleaned[df_cleaned['Data_Value'] != mean_value]
 
percent_df.reset_index(drop = True,inplace=True)
percent_df['stratification_id'] = percent_df.index
 
new_column_names = {
    'RowId': 'fact_id',
	'Data_Value':'data_value',
	'Low_Confidence_Limit': 'low_confidence_limit',
	'High_Confidence_Limit': 'high_confidence_limit',
	'TopicID': 'topic_id',
	'ClassID': 'class_id', 
	'QuestionID': 'question_id',
    'LocationID': 'location_id',
    'YearStart': 'year_start',
    'YearEnd': 'year_end',
    'StratificationID1': 'stratification1_id',
    'StratificationID2': 'stratification2_id',
    'StratificationCategoryID2': 'stratification_categoryid2'
}
 
percent_df = percent_df.rename(columns=new_column_names)
new_order = ['fact_id', 'data_value', 'low_confidence_limit', 'high_confidence_limit', 'location_id', 'topic_id', 'class_id', 'year_start', 'year_end','question_id','stratification1_id','stratification2_id','stratification_categoryid2']
percent_df = percent_df[new_order]
percent_df.head(20)




#Creating mean fact table
mean_df = df_cleaned[df_cleaned['Data_Value_Type'] != 'Percentage']

new_column_names = {
    'RowId': 'fact_id',
	'Data_Value':'data_value',
	'Low_Confidence_Limit': 'low_confidence_limit',
	'High_Confidence_Limit': 'high_confidence_limit',
	'TopicID': 'topic_id',
	'ClassID': 'class_id', 
	'QuestionID': 'question_id',
    'LocationID': 'location_id',
    'YearStart': 'year_start',
    'YearEnd': 'year_end',
    'StratificationID1': 'stratification1_id',
    'StratificationID2': 'stratification2_id',
    'StratificationCategoryID2': 'stratification_categoryid2'
}

mean_df = mean_df.rename(columns=new_column_names)
new_order = ['fact_id', 'data_value', 'low_confidence_limit', 'high_confidence_limit', 'location_id', 'topic_id', 'class_id', 'year_start', 'year_end','question_id','stratification1_id','stratification2_id','stratification_categoryid2']
mean_df = mean_df[new_order]
mean_df.head(10)
mean_df.info()





# Database connection URL
# Replace the placeholders with your actual database credentials
pwd = 'Hide'
database_url = f'postgresql://laishan:{pwd}@cis9440baruchdw.postgres.database.azure.com/postgres'

# Create a SQLAlchemy engine
engine = create_engine(database_url)



#insert dimention table and fact table
unique_topic_df.to_sql('dim_topic', con=engine, schema='alzheimer', if_exists='append', index=False)
unique_topic_df.to_csv("topic.csv",index=False)
unique_class_df.to_sql('dim_class', con=engine, schema='alzheimer', if_exists='append', index=False)
unique_class_df.to_csv("class.csv",index=False)
unique_questions_df.to_sql('dim_question', con=engine, schema='alzheimer', if_exists='append', index=False)
unique_questions_df.to_csv("questions.csv",index=False)
location_lookup_df.to_sql('dim_location', con=engine, schema='alzheimer', if_exists='append', index=False)
location_lookup_df.to_csv("location.csv",index=False)
unique_stratification_category2_df.to_sql('dim_stratification_category', con=engine, schema='alzheimer', if_exists='append', index=False)
unique_stratification_category2_df.to_csv("stratification_category2.csv",index=False)
unique_stratification1_df.to_sql('dim_stratification1', con=engine, schema='alzheimer', if_exists='append', index=False)
unique_stratification1_df.to_csv("stratification1.csv",index=False)
unique_stratification2_df.to_sql('dim_stratification2', con=engine, schema='alzheimer', if_exists='append', index=False)
unique_stratification2_df.to_csv("stratification2.csv",index=False)
unique_syear_df.to_sql('dim_year', con=engine, schema='alzheimer', if_exists='append', index=False)
unique_syear_df.to_csv("syear.csv",index=False)
percent_df.to_sql('facts_data_value_percentage', con=engine, schema='alzheimer', if_exists='append', index=False)
percent_df.to_csv("percent.csv",index=False)
mean_df.to_sql('facts_data_value_mean', con=engine, schema='alzheimer', if_exists='append', index=False)
mean_df.to_csv("mean.csv",index=False)
