-- Databricks notebook source
-- MAGIC %fs ls /databricks-datasets/airlines/part-00000

-- COMMAND ----------

-- MAGIC %fs head /databricks-datasets/airlines/part-00000

-- COMMAND ----------

-- MAGIC %run ./Include

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC
-- MAGIC print( get_user_name() )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.text('username', get_user_name())

-- COMMAND ----------

create schema ${username}

-- COMMAND ----------

CREATE TABLE ${username}.airlines_pt0_csv
USING
  CSV
LOCATION
  '/databricks-datasets/airlines/part-00000'
OPTIONS
  (inferSchema = True,
  header = True)

-- COMMAND ----------

SELECT * FROM ${username}.airlines_pt0_csv

-- COMMAND ----------

SELECT
  SUM(CASE WHEN DepDelay > 0 THEN 1 ELSE 0 END) AS num_delayed_flights
FROM
  odl_instructor_927871.airlines_pt0_csv
