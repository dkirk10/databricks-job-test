# Databricks notebook source
# Import necessary libraries
import requests
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

# Initialize Spark session
spark = SparkSession.builder.appName("ImportRawData").getOrCreate()

# COMMAND ----------

# Define the API endpoint
api_url = "https://randomuser.me/api/?results=5"

# COMMAND ----------

# Fetch the data from the API
response = requests.get(api_url)
data = response.json()

# COMMAND ----------

# Extract the relevant data (assuming we want the 'results' part of the response)
users = data['results']

# COMMAND ----------

# Convert the data to a DataFrame
# Define the schema
schema = StructType([
    StructField("gender", StringType(), True),
    StructField("title", StringType(), True),
    StructField("first", StringType(), True),
    StructField("last", StringType(), True),
    StructField("street", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("postcode", StringType(), True),
    StructField("email", StringType(), True),
    StructField("username", StringType(), True),
    StructField("password", StringType(), True),
    StructField("dob", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("cell", StringType(), True),
    StructField("nat", StringType(), True)
])

# Create a list of rows to conform to the schema
rows = []
for user in users:
    rows.append((
        user['gender'],
        user['name']['title'],
        user['name']['first'],
        user['name']['last'],
        user['location']['street']['name'],
        user['location']['city'],
        user['location']['state'],
        user['location']['postcode'],
        user['email'],
        user['login']['username'],
        user['login']['password'],
        user['dob']['date'],
        user['phone'],
        user['cell'],
        user['nat']
    ))

# Create DataFrame
raw_df = spark.createDataFrame(rows, schema)

# COMMAND ----------

# Show a sample of the data
raw_df.show(5)

# COMMAND ----------

# Define the S3 bucket path
s3_path = "s3://ritesh-databricks-job-bucket/raw_data_parquet"

# Save the raw DataFrame to the S3 location as Parquet format
raw_df.write.format("parquet").mode("overwrite").save(s3_path)


# COMMAND ----------

from pyspark.sql.functions import col

# Display the data using the Databricks display function for better readability
display(raw_df)

# Alternatively, if you want to select specific columns and rename them for a cleaner display
display(raw_df.select(
    col("gender").alias("Gender"),
    col("title").alias("Title"),
    col("first").alias("First Name"),
    col("last").alias("Last Name"),
    col("street").alias("Street"),
    col("city").alias("City"),
    col("state").alias("State"),
    col("postcode").alias("Postcode"),
    col("email").alias("Email"),
    col("username").alias("Username"),
    col("password").alias("Password"),
    col("dob").alias("Date of Birth"),
    col("phone").alias("Phone"),
    col("cell").alias("Cell"),
    col("nat").alias("Nationality")
))

