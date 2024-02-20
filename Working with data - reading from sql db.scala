// Databricks notebook source
// MAGIC %md
// MAGIC code reference for sql connector - https://learn.microsoft.com/en-us/azure/hdinsight/spark/apache-spark-connect-to-sql-database

// COMMAND ----------

// Declare the values for your database

val jdbcUsername = "Dp203admin"
val jdbcPassword = dbutils.secrets.get(scope="datalakedemodp203_key", key="dp-password")
val jdbcHostname = "dp203section4.database.windows.net" //typically, this is in the form or servername.database.windows.net
val jdbcPort = 1433
val jdbcDatabase ="adf-db"

// COMMAND ----------

import java.util.Properties

val jdbc_url = s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=60;"
val connectionProperties = new Properties()
connectionProperties.put("user", s"${jdbcUsername}")
connectionProperties.put("password", s"${jdbcPassword}")

// COMMAND ----------

val sqlTableDF = spark.read.jdbc(jdbc_url, "dbo.BlobDiagnostics", connectionProperties)

// COMMAND ----------

sqlTableDF.printSchema

// COMMAND ----------

sqlTableDF.show(10)

// COMMAND ----------

sqlTableDF.select("Category", "OperationName").show(10)

// COMMAND ----------

display(sqlTableDF)
