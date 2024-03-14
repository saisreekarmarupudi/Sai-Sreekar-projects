-- Databricks notebook source
CREATE OR REFRESH STREAMING LIVE TABLE network_data_na_bronze_1
COMMENT "Ingesting the network data using autoloader"
AS 
SELECT *
FROM cloud_files("dbfs:/mnt/volumes/network_data/*", "json", map("cloudFiles.inferColumnTypes", "true"))

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE network_data_na_bronze_2
COMMENT "Partition data of bronze network data"
AS
SELECT *,
       DATE_FORMAT(timestamp, 'MMddyyyy') AS partition_date,
       DATE_FORMAT(timestamp, 'HH') AS partition_hour
FROM STREAM(LIVE.network_data_na_bronze_1)

-- COMMAND ----------

CREATE STREAMING LIVE TABLE network_data_na_silver_1
(CONSTRAINT valid_partition_date EXPECT (to_date(partition_date, 'MMddyyyy') IS NOT NULL) ON VIOLATION DROP ROW,
CONSTRAINT valid_source_port EXPECT (source_port > 0 AND source_port <= 9999999999) ON VIOLATION DROP ROW
)
AS
SELECT 
    t.*,
    user_detail.user_id AS user_id,
    user_detail.name AS user_name,
    user_detail.email AS user_email,
    user_detail.role AS user_role,
    user_detail.department AS user_department,
    user_detail.city AS user_city,
    user_detail.location AS user_location
FROM 
    STREAM(LIVE.network_data_na_bronze_2) t
LATERAL VIEW OUTER EXPLODE(users) exploded_users AS user_detail;

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE network_data_na_silver_2
AS SELECT n.*, d.firewall_vendor, d.firewall_vendor_version
FROM STREAM(LIVE.network_data_na_silver_1) n LEFT OUTER JOIN device_info d

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE hourly_aggregation_na_gold
AS SELECT location, partition_date, partition_hour, count(threat_description) AS threat_description
FROM LIVE.network_data_na_silver_2
GROUP BY location, partition_date, partition_hour