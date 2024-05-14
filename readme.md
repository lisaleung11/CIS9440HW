# Overview
This dataset contains information related to various health-related issues among older adults, collected over different years and locations. This homework is using azure to store the data, DBSchema to generate the SQL code, DataGrip to execute SQL queries and finally Tableau to do the visualization. 

# Scripts
Scripts folder contain config.json file which is the connection string to azure storage.

ETL.py contains the whole ETL processes such as pulling the data from cdc.gov, cleaning and reformatting the data, finally loading the dimension and fact tables to postgres. https://github.com/lisaleung11/CIS9440HW/blob/main/scripts/ETL.py

ETL.ipynb is the jupyter notebote file for block testing.

# Models
This folder contains the sql schema which is used to create dimension and fact table.
https://github.com/lisaleung11/CIS9440HW/blob/main/models/alzheimer_data_model.sql

# Docs
This folder contains:

-csv file from dimension and fact tables

-data dictionary 
https://github.com/lisaleung11/CIS9440HW/blob/main/docs/data%20dictionary.txt

-visualization PowerPoint 
https://github.com/lisaleung11/CIS9440HW/blob/main/docs/Alzheimer%20powerpoint.pptx

# Dimensional modeling
![image](https://github.com/lisaleung11/CIS9440HW/blob/main/dimension%20modeling.png)
