# Databricks notebook source
# Databricks notebook source
# MAGIC %md
# MAGIC # Transform Data

# COMMAND ----------

# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# COMMAND ----------

# Initialize Spark session
spark = SparkSession.builder.appName("TransformData").getOrCreate()

# COMMAND ----------

# Define the S3 bucket path for raw data
raw_data_path = "s3://ritesh-databricks-job-bucket/raw_data_parquet"

# Load the raw data from the Parquet table
raw_df = spark.read.format("parquet").load(raw_data_path)

# COMMAND ----------

# Select the specified columns
transformed_df = raw_df.select(
    col("gender"),
    col("first").alias("first_name"),
    col("last").alias("last_name"),
    col("dob").alias("date_of_birth"),
    col("email"),
    col("username"),
    col("password")
)

# COMMAND ----------

# Show a sample of the transformed data
display(transformed_df)

# COMMAND ----------

# Define the S3 bucket path for transformed data
transformed_data_path = "s3://ritesh-databricks-job-bucket/transformed_data_parquet"

# Save the transformed DataFrame to the S3 location as Parquet format
transformed_df.write.format("parquet").mode("overwrite").save(transformed_data_path)

