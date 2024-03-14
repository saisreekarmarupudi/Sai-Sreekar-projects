# Databricks notebook source
pip install faker


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import faker
from datetime import timedelta
import random

# Initialize Faker and SparkSession
fake = faker.Faker()
spark = SparkSession.builder.appName("Data Generation with Faker").getOrCreate()

# Generate data
data = []
for _ in range(1000):  # Assuming you want to generate 100 records
    firewall_name = "FW" + str(random.randint(1, 100))  # Example rule for firewall_name
    firewall_vendor = fake.company()
    firewall_vendor_version = fake.bothify(text='??.#.#', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZ')
    last_updated_date = fake.date_between(start_date="-5y", end_date="today")
    do_not_use_after_date = last_updated_date + timedelta(days=random.randint(365, 365*5))
    
    data.append((firewall_name, firewall_vendor, firewall_vendor_version, last_updated_date, do_not_use_after_date))

# Define DataFrame schema
schema = ["firewall_name", "firewall_vendor", "firewall_vendor_version", "last_updated_date", "do_not_use_after_date"]

# Create DataFrame
df = spark.createDataFrame(data, schema=schema)

# Show the DataFrame
df.show(truncate=False)


# COMMAND ----------

df.write.format("delta").mode("append").saveAsTable("device_info")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from device_info

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import faker
from datetime import timedelta
import random

# Initialize Faker and SparkSession
fake = faker.Faker()
spark = SparkSession.builder.appName("Data Generation with Faker").getOrCreate()

# Generate data
data = []
for _ in range(1000):  # Assuming you want to generate 100 records
    firewall_name = "FW" + str(random.randint(1, 100))  # Example rule for firewall_name
    firewall_vendor = fake.company()
    firewall_vendor_version = fake.bothify(text='??.#.#', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZ')
    last_updated_date = fake.date_between(start_date="-5y", end_date="today")
    do_not_use_after_date = last_updated_date + timedelta(days=random.randint(365, 365*5))
    
    data.append((firewall_name, firewall_vendor, firewall_vendor_version, last_updated_date, do_not_use_after_date))

# Define DataFrame schema
schema = ["firewall_name", "firewall_vendor", "firewall_vendor_version", "last_updated_date", "do_not_use_after_date"]

# Create DataFrame
df = spark.createDataFrame(data, schema=schema)

# Show the DataFrame
df.show(truncate=False)

# COMMAND ----------

