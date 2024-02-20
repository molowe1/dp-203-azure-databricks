// Databricks notebook source
// MAGIC %sql
// MAGIC USE appdb

// COMMAND ----------

// MAGIC %sql
// MAGIC DROP TABLE logdata

// COMMAND ----------

// Lab - Few functions on dates

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

spark.conf.set(
    "fs.azure.account.key.datalakedemodp203.dfs.core.windows.net",
    dbutils.secrets.get(scope="datalakedemodp203_key", key="datalakedemodp203-secret"))

val file_location = "/mnt/dp203_datalake/Log.csv"
val file_type = "csv"

val dataSchema = StructType(Array(    
    StructField("id", IntegerType, true),
    StructField("Correlationid", StringType, true),
    StructField("Operationname", StringType, true),
    StructField("Status", StringType, true),
    StructField("Eventcategory", StringType, true),
    StructField("Level", StringType, true),
    StructField("Time", TimestampType, true),
    StructField("Subscription", StringType, true),
    StructField("Eventinitiatedby", StringType, true),
    StructField("Resourcetype", StringType, true),
    StructField("Resourcegroup", StringType, true),
    StructField("Resource", StringType, true)))


val df = spark.read.format(file_type).
options(Map("header"->"true")).
schema(dataSchema).
load(file_location)

// COMMAND ----------

df.write.saveAsTable("appdb.logdata")
