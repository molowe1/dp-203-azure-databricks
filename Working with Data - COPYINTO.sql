-- Databricks notebook source
-- Lab - Using the COPY INTO command

CREATE DATABASE appdb

       

-- COMMAND ----------

USE appdb

-- COMMAND ----------

CREATE TABLE logdata


-- COMMAND ----------

-- MAGIC %scala
-- MAGIC spark.conf.set(
-- MAGIC     "fs.azure.account.key.datalakedemodp203.dfs.core.windows.net",
-- MAGIC     "aTVPvtKPOE2ZdZm8o2uCG42KJM93qYnySCktFMb5TEjQiNbAoTf35s1Ha1ATgAg12cPbXxki7In++AStaTR/Uw==")

-- COMMAND ----------

COPY INTO logdata
FROM 'abfss://parquet@datalakedemodp203.dfs.core.windows.net/Log.parquet'
FILEFORMAT = PARQUET
COPY_OPTIONS ('mergeSchema' = 'true'); 

-- COMMAND ----------

select * from logdata
