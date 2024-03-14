# Databricks notebook source
# MAGIC %md
# MAGIC # We are creating a Realtime Network Threat Analysis Dashboard 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze Layer (load Raw Data)
# MAGIC ### We are loading network events from all firewalls in the company to ADLS folder on Azure
# MAGIC ### Bronze job is using Autoloader to load the events using the given schema
# MAGIC ### Bronze Job is storing the data in Delta table with partitions

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver Layer Notebook 1 (Data Validations)
# MAGIC ### Notebook 1 is loading the new data loaded into bronze layer since the last run
# MAGIC ### It explodes the array (user_detail) and flattens them as columns
# MAGIC ### Notebook 1 is doing the validations on the columns
# MAGIC ### Notebook 1 is writing the valid records to a delta table with partitions
# MAGIC ### Notebook 1 is writing the invalid records to a different table

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver Layer Notebook 2 (Data Enrichment)
# MAGIC ### Notebook 2 is doing data enrichment by joining with other tables

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold Layer Notebook 1(KPI or Metric aggregates calculation)
# MAGIC #### Notebook is calculatin the aggreates of threat from a location by hour and week and month and saving to three different tables.

# COMMAND ----------

# MAGIC %md
# MAGIC # Dashboard and Query (Metric visualization or KPI visualization)
# MAGIC ### We are creating the querys on the Gold table for various metrics display or visualization
# MAGIC ### We are using these queries to create visulaizations on the dashboard 