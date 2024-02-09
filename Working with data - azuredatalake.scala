// Databricks notebook source
// Lab - Reading from Azure Data Lake

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


spark.conf.set(
    "fs.azure.account.key.datalakedemodp203.dfs.core.windows.net",
    "aTVPvtKPOE2ZdZm8o2uCG42KJM93qYnySCktFMb5TEjQiNbAoTf35s1Ha1ATgAg12cPbXxki7In++AStaTR/Uw==")

val file_location = "abfss://csv@datalakedemodp203.dfs.core.windows.net/Log.csv"
val file_type = "csv"

val dataSchema = StructType(Array(    
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


display(df)
