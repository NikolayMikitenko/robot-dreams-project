# Robot dreams final project

## COPY ALL FILES FROM FOLDER dags to airflow/dags and add PYTHONPATH to common folder!!!

## Pipelines
* out_of_stock_pipeline - load data from http to silver layer
* postgres_data_pipeline - load dshop data to silver layer and after to DWH 

## Folders contents
* scripts - folder with sql scripts for DWH
* dags - folder with dags data pipelines and their configs and python modules
* dags/common - folder with dags modules
* dags/config - folder with dag configs

## Files contents
* dags/out_of_stock_pipeline - load data from http to silver layer
* dags/postgres_data_pipeline - load dshop data to silver layer and after to DWH 
* config/config.yaml - file with http aplication config
* dags/common/config.py - class for work with application config
* dags/common/http_to_hdfs_operator.py - class for call http and save result to HDFS
* dags/common/spark_bronze_json_to_silver.py - script with functions for load http data in json from bronze to silver
* dags/common/spark_bronze_to_silver.py - script with functions for load DB dshop data from bronze to silver
* dags/common/spark_postgres_to_bronze.py - script with functions for load DB dshop data from DB to bronze
* dags/common/spark_silver_to_dwh.py - script with functions for data from silver to DWH

## DWH prepare logic and load available in file:
https://github.com/NikolayMikitenko/robot-dreams-spark/blob/main/Homework_Project_Spark.ipynb

## Data cleaning
* Aggregation rows by orders in one row with summarize of quantity
* Deduplication in API and loading into one table in Cleaned

## Data preparation for DWH
* load dimensions
* load facts
* for facts check missing keys in dimension and insert Unknown records for missing keys
