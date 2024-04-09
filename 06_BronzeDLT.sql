-- Databricks notebook source
CREATE OR REFRESH STREAMING TABLE airlines_bronze
AS
SELECT 
*
FROM cloud_files('/databricks-datasets/airlines/part-0000[0-10]', "csv", map(
  "header", "true",
  "inferSchema", "true"))

-- COMMAND ----------


