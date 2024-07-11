# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG ritesh_databricks_prod_ctlog;
# MAGIC USE SCHEMA ritesh_databricks_job_schema;

# COMMAND ----------


"""
%sql
CREATE TABLE transformed_user_data_parquet (
    gender STRING,
    first_name STRING,
    last_name STRING,
    date_of_birth STRING,
    email STRING,
    username STRING,
    password STRING
)
USING PARQUET 
LOCATION 's3://ritesh-databricks-job-bucket/processed_data_parquet';
"""


# COMMAND ----------

# COMMAND ----------

# Import necessary libraries
from pyspark.sql import SparkSession

# COMMAND ----------

# Initialize Spark session
spark = SparkSession.builder.appName("ProcessData").getOrCreate()

# COMMAND ----------

# Define the S3 bucket path for transformed data
transformed_data_path = "s3://ritesh-databricks-job-bucket/transformed_data_parquet"

# Load the transformed data from the Parquet table
transformed_df = spark.read.format("parquet").load(transformed_data_path)

# COMMAND ----------

# Show a sample of the transformed data
display(transformed_df)

# COMMAND ----------

# Define the external table name
external_table_name = "transformed_user_data"

# Write the DataFrame to the external table (if not done by the above step)
# Note: This step might not be necessary if the table creation already includes data
transformed_df.write.mode("overwrite").saveAsTable(external_table_name)

# COMMAND ----------

# Verify that the data has been written to the external table
# Query the table and show the data
query_result = spark.sql(f"SELECT * FROM {external_table_name}")
display(query_result)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM transformed_user_data;
