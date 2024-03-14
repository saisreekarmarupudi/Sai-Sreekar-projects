# Databricks notebook source
# MAGIC %md
# MAGIC ## This notebook will do the explode on the users array by loading data from bronze table.
# MAGIC ### This notebook also is responsible for data validation on all columns of the data
# MAGIC ### This notebook will write all non confirming or invalid records to error table and all valid records to the valid table 

# COMMAND ----------

# MAGIC %md
# MAGIC # note that this notebook when it starts will try to see what was the max date from the last run. It will use that date and reads only data greater than that max date from bronze table.

# COMMAND ----------

df_max_date = spark.sql("select max(timestamp) from network_data3")
max_date_from_table = df_max_date.select(df_max_date.columns[0]).first()[0]
print("max date from data is ",max_date_from_table)

delta_table_path = "dbfs:/mnt/volumes/network_data/silver_max_date"
df = spark.sql("select * from network_data3")
try:
    last_max_date_df = spark.read.format("delta").load(delta_table_path)
    display(last_max_date_df)
    max_date_saved = last_max_date_df.select(last_max_date_df.columns[0]).first()[0]
    print(max_date_saved)
    if(max_date_saved!=""):
         print("using last_max_date_df",max_date_saved)
         sql_query = "SELECT * FROM network_data3 WHERE `timestamp` > '{}'".format(max_date_saved)
         df = spark.sql(sql_query)
    
except Exception as e:
    print("ok",e)




# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #Explode logic for users array

# COMMAND ----------


from pyspark.sql.functions import explode, col

# Assuming df is your DataFrame and 'users' is the column to be flattened
# Explode the 'users' array column to create a new row for each user
exploded_df = df.selectExpr("*", "explode(users) as user_detail")

# Now, select the struct fields as separate columns along with any other columns you need
flattened_df = exploded_df.select(
    col("user_detail.user_id"),
    col("user_detail.name"),
    col("user_detail.email"),
    col("user_detail.role"),
    col("user_detail.department"),
    "*",
    # Include other columns from the original DataFrame as needed
)






# COMMAND ----------

display(flattened_df)

# COMMAND ----------

df = flattened_df.drop("users","user_detail")


# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data validation on the partition_date column. We are making sure the date is not null and is in the expected format of MMddyyyy

# COMMAND ----------

def validate_partition_date(df):
    from pyspark.sql.functions import to_date, col
    # Specify the expected date format
    dateFormat = "MMddyyyy"

    # Convert the date string to a date object using the specified format
    # This will result in null for records that do not match the format
    df = df.withColumn("parsed_date", to_date(col("partition_date"), dateFormat))
    df_good = df.filter(col("parsed_date").isNotNull())
    df_error = df.filter(col("parsed_date").isNull())
    return df_good

# COMMAND ----------

def validate_source_port(df):
    positive_df = df.filter(col("source_port") > 0 && col("source_port") > 9999999999)
    
    return positive_df

# COMMAND ----------

df_good = validate_partition_date(df)
df_good = validate_source_port(df_good)


# COMMAND ----------

display(df_error)

# COMMAND ----------

display(df_good)

# COMMAND ----------

# MAGIC %md
# MAGIC # We are saving the valid data to Silver_zero table with partitions partition_date and partition_hour
# MAGIC ### Errors are being written to bad_siver_zero_1 table

# COMMAND ----------

# Write records with good date format to the good table
df_good.write.format("delta").mode("append").partitionBy("partition_date", "partition_hour").saveAsTable("silver_zero")

# Write error records to the error table
if(df_error.count==0):
    print("no errors")
else:    
   try:
       # look at merge schema as it is updating the schema of the underlyng table to match with schema on the dataframe
      df_error.write.format("delta").mode("append").option("mergeSchema", "true").partitionBy("partition_date").saveAsTable("bad_silver_zero_1")
   except:
      print("exception while writing to error path",e)

# COMMAND ----------

display(df_max_date)
df_max_date_df = df_max_date.withColumnRenamed("max(timestamp)", "max_date")

# COMMAND ----------

# MAGIC %md
# MAGIC # This save of max date is very important for next run. As it uses this date and selects all data greater than this for the next run.

# COMMAND ----------

# Write the DataFrame to Delta table
df_max_date_df.write.format("delta").mode("overwrite").save(delta_table_path)

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from network_data3 where partition_date is null

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from bad_silver_zero_1

# COMMAND ----------

