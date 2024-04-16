-- Databricks notebook source
-- MAGIC %run ./Include

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.text('username', get_user_name())

-- COMMAND ----------

SELECT * from ${username}.Airlines_Silver

-- COMMAND ----------

CREATE TABLE ${username}.Airlines_Gold AS
SELECT UniqueCarrier, round(count_if(WasLate) / count(*) * 100) as LatePercentage from ${username}.Airlines_Silver Group By UniqueCarrier

-- COMMAND ----------

SELECT UniqueCarrier, LatePercentage
FROM ${username}.Airlines_Gold
