# Databricks notebook source
# MAGIC %md
# MAGIC #Add the logic to use the max data and should not take all data from silver_zero

# COMMAND ----------

df = spark.sql("select * from silver_zero")

# COMMAND ----------

Source: silver_zero
Target: silver_final_enriched

last_successful_date for this job

if I have last_successful_date and if it is populated
use that to get the data from silver_zero and apply transformations
and write to silver_final_enriched

else if last_succesful_date is not populated
take all data from silver_zero


update the last_successful_date to the max(timestamp) in silver_final_enriched

# COMMAND ----------

df_max_date = spark.sql("select max(timestamp) from silver_zero")
max_date_from_table = df_max_date.select(df_max_date.columns[0]).first()[0]
print("max date from data is ",max_date_from_table)

delta_table_path = "dbfs:/mnt/volumes/network_data/silver1_max_date"
df = spark.sql("select * from silver_zero")
try:
    last_max_date_df = spark.read.format("delta").load(delta_table_path)
    display(last_max_date_df)
    max_date_saved = last_max_date_df.select(last_max_date_df.columns[0]).first()[0]
    print(max_date_saved)
    if(max_date_saved!=""):
         print("using last_max_date_df",max_date_saved)
         sql_query = "SELECT * FROM silver_zero WHERE `timestamp` > '{}'".format(max_date_saved)
         df = spark.sql(sql_query)
    
except Exception as e:
    print("ok",e)


# COMMAND ----------

display(df)

# COMMAND ----------

df_device_data = spark.sql("select * from device_info")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Logic to remove duplicate data by firewall_name in the device_info table. The reason is if there are duplicates. When we do a left join it will cause our network records also to get duplicated.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as F
device_df = spark.sql("select * from device_info")
SELE
window_spec = Window.partitionBy("firewall_name").orderBy(F.desc("last_updated_date"))

# Add a "rank" column based on the ranking of "value" within each "firewall_name" partition
df_ranked = df_device_data.withColumn("rank", F.rank().over(window_spec))

# Filter for records where rank is 1
result = df_ranked.filter("rank = 1")

# Drop the "rank" column if not needed in the final result
result = result.drop("rank")

# Show the result
#result.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENEDED device_info

# COMMAND ----------

display(result)

# COMMAND ----------

# Perform the left join
joined_df = df.join(result, on="firewall_name", how="left")

joined_df.show(5)


# COMMAND ----------

display(joined_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # save this data to a table

# COMMAND ----------

joined_df.write.format("delta").mode("append").option("mergeSchema", "true").partitionBy("partition_date","partition_hour").saveAsTable("silver_final_enriched")

# COMMAND ----------

display(df_max_date)
df_max_date_df = df_max_date.withColumnRenamed("max(timestamp)", "max_date")

# COMMAND ----------

df_max_date_df.write.format("delta").mode("overwrite").save(delta_table_path)