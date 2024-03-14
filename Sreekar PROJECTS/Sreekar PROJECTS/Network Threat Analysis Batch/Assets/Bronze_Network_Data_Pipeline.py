# Databricks notebook source
# MAGIC %md
# MAGIC #This notebook will create the schema matching the expected data to read for ADLS Location
# MAGIC ## Obeserve that users is an array with user_id, name, email , role , department as attributes.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, TimestampType

schema = StructType([
    StructField("timestamp", TimestampType(), True),
    StructField("source_ip", StringType(), True),
    StructField("destination_ip", StringType(), True),
    StructField("source_port", IntegerType(), True),
    StructField("destination_port", IntegerType(), True),
    StructField("protocol", StringType(), True),
    StructField("action", StringType(), True),
    StructField("bytes", IntegerType(), True),
    StructField("packets", IntegerType(), True),
    StructField("duration", StringType(), True),
    StructField("rule_name", StringType(), True),
    StructField("rule_id", StringType(), True),
    StructField("policy_name", StringType(), True),
    StructField("firewall_name", StringType(), True),
    StructField("direction", StringType(), True),
    StructField("application", StringType(), True),
    StructField("user", StringType(), True),
    StructField("location", StringType(), True),
    StructField("threat_level", StringType(), True),
    StructField("threat_description", StringType(), True),
    StructField("users", ArrayType(StructType([
        StructField("user_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("role", StringType(), True),
        StructField("department", StringType(), True),
    ]), True), True)
])


# COMMAND ----------

# MAGIC %md
# MAGIC # This cell is using Autoloader and reading the files. 
# MAGIC ##All bad records will be written to badRecordsPath
# MAGIC ##Autoloader should always use file notifications rather than file listing for production.
# MAGIC ## File listing will be too expensive but best for production purpose
# MAGIC ## When file notifications is enabled. The new files coming into ADLS folder are sent as events into event grid. Databricks will take the new file names from eventgrid queue rather than doing a file listing

# COMMAND ----------

from pyspark.sql.functions import date_format

inputPath = "dbfs:/mnt/volumes/network_data/*"

df = (spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "json")
      #.option("cloudFiles.schemaLocation", "dbfs:/mnt/volumes/schema_location")\
      # pass all the options to use file events instead of file listing
      .option("cloudFiles.useIncrementalListing", "auto")\
      .option("badRecordsPath","dbfs:/mnt/volumes/error_data")\
      .schema(schema)
      .load(inputPath))


# COMMAND ----------

display(df)

# COMMAND ----------

df_transformed = df.withColumn("partition_date", date_format("timestamp", "MMddyyyy")) \
                   .withColumn("partition_hour", date_format("timestamp", "HH"))


# COMMAND ----------

display(df_transformed)

# COMMAND ----------

outputPath = "dbfs:/delta-table/network_data3"
checkpointLocation = "dbfs:/mnt/volumes/checkpoints3"

(df_transformed.writeStream
    .format("delta")
    .option("checkpointLocation", checkpointLocation)
    .option("cloudFiles.maxFilesPerTrigger","100")
    .partitionBy("partition_date", "partition_hour")
    .trigger(availableNow=True)
    .start(outputPath))


# COMMAND ----------

# MAGIC %sql
# MAGIC update network_data3 set partition_date = " " where partition_hour = 23

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC select * from network_data3

# COMMAND ----------

spark.sql("""
CREATE TABLE network_data3
USING DELTA
LOCATION 'dbfs:/delta-table/network_data3'
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from network_data2

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended network_data2