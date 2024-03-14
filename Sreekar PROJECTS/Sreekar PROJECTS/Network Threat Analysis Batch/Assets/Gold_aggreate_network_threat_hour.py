# Databricks notebook source
# MAGIC %md
# MAGIC ### Add logic to process only new data
# MAGIC ### Create further gold notebooks for each metric and store the results in appropriate table

# COMMAND ----------

df = spark.sql("select * from silver_final_enriched")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(threat_description) as threat_description,location,partition_date,partition_hour from silver_final_enriched where threat_description='Suspicious Activity Detected' group by location, partition_date, partition_hour

# COMMAND ----------



# COMMAND ----------

df_hourly_aggregated = spark.sql("select count(threat_description) as threat_description,location,partition_date,partition_hour from silver_final_enriched where threat_description='Suspicious Activity Detected' group by location, partition_date, partition_hour")

# COMMAND ----------

df_hourly_aggregated.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable("gold_network_threat_hourly")

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
#df_hourly_aggregated = df_hourly_aggregated.withColumn('partition_date', F.to_date(df_hourly_aggregated['partition_date']))
df_hourly_aggregated = df_hourly_aggregated.withColumn('partition_date', F.to_date(F.unix_timestamp(df_hourly_aggregated['partition_date'], 'MMddyyyy').cast('timestamp')))

# Calculate week start date using trunc function
df_hourly_aggregated = df_hourly_aggregated.withColumn('week_start_date', F.trunc('partition_date', 'week'))

# Group by week, location, and partition_hour and count the threat_description
result = df_hourly_aggregated.groupBy('week_start_date', 'location') \
           .agg(F.count('threat_description').alias('threat_description_count'))

# Show the result
result.show()

# COMMAND ----------

display(result)

# COMMAND ----------

result.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable("network_threat_weekly")