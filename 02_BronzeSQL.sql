-- Databricks notebook source
-- MAGIC %run ./Include

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.text('username', get_user_name())
-- MAGIC print ( get_user_name())

-- COMMAND ----------

CREATE TABLE ${username}.Airlines_Bronze AS
SELECT * FROM ${username}.airlines_pt0_csv

-- COMMAND ----------

SELECT * FROM ${username}.Airlines_Bronze

-- COMMAND ----------

describe detail ${username}.airlines_pt0_csv

-- COMMAND ----------

describe detail ${username}.Airlines_Bronze

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/odl_instructor_927871.db/airlines_bronze

-- COMMAND ----------


