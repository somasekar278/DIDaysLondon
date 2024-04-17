-- Databricks notebook source
CREATE OR REFRESH STREAMING TABLE airlines_bronze
AS
SELECT 
*
FROM cloud_files('/databricks-datasets/airlines/part-0000[0-10]', "csv", map(
  "header", "true",
  "inferSchema", "true"))

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE Airlines_Silver
AS
SELECT 
*,
ArrDelay > 0 OR DepDelay > 0 AS WasLate 
FROM 
LIVE.Airlines_Bronze

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE Airlines_Gold AS
SELECT UniqueCarrier, round(count_if(WasLate) / count(*) * 100) as LatePercentage from LIVE.Airlines_Silver Group By UniqueCarrier
