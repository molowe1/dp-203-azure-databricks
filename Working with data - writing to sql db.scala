// Databricks notebook source
// MAGIC %md
// MAGIC code reference for sql connector - https://learn.microsoft.com/en-us/azure/hdinsight/spark/apache-spark-connect-to-sql-database

// COMMAND ----------

// Declare the values for your database

val jdbcUsername = "Dp203admin"
val jdbcPassword = "demo@203password"
val jdbcHostname = "dp203section4.database.windows.net" //typically, this is in the form or servername.database.windows.net
val jdbcPort = 1433
val jdbcDatabase ="app-db"

// COMMAND ----------

import java.util.Properties

val jdbc_url = s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=60;"
val connectionProperties = new Properties()
connectionProperties.put("user", s"${jdbcUsername}")
connectionProperties.put("password", s"${jdbcPassword}")

// COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.datalakedemodp203.dfs.core.windows.net",
    "aTVPvtKPOE2ZdZm8o2uCG42KJM93qYnySCktFMb5TEjQiNbAoTf35s1Ha1ATgAg12cPbXxki7In++AStaTR/Uw==")

// COMMAND ----------

val userSchema = spark.read.option("header", "true").option("inferSchema","true").csv("abfss://csv@datalakedemodp203.dfs.core.windows.net/Customer/Customer02.csv").schema
val readDf = spark.read.format("csv").option("header", "true").option("inferSchema","true").schema(userSchema).load("abfss://csv@datalakedemodp203.dfs.core.windows.net/Customer/Customer02.csv")

// COMMAND ----------

readDf.createOrReplaceTempView("tempcust_table")
spark.sql("create table IF NOT EXISTS test as select * from tempcust_table")

// COMMAND ----------

spark.table("test").write.jdbc(jdbc_url, "Dbo.DimCust_Test", connectionProperties)

// COMMAND ----------

val sqlTableDF = spark.read.jdbc(jdbc_url, "Dbo.DimCust_Test", connectionProperties)

// COMMAND ----------

sqlTableDF.printSchema

// COMMAND ----------

sqlTableDF.show(5)

// COMMAND ----------

// MAGIC %md
// MAGIC sqlTableDF.select("Category", "OperationName").show(10)

// COMMAND ----------

display(sqlTableDF)
