// Databricks notebook source
// Lab - Few functions on dates

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

val file_location = "/FileStore/csv/Log.csv"
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

display(df)

// COMMAND ----------



// COMMAND ----------

// Here we are getting the year,month and day of year
import org.apache.spark.sql.functions._
display(df.select(year(col("Time")),month(col("Time")),dayofyear(col("Time"))))

// COMMAND ----------

// Use alias if you want to give more meaningful names to the output columns
display(df.select(year(col("Time")).alias("Year"),month(col("Time")).alias("Month"),dayofyear(col("Time")).alias("Day of Year")))

// COMMAND ----------

// If you want to convert the date to a particular format
display(df.select(to_date(col("Time"),"dd-mm-yyyy").alias("Date")))

// COMMAND ----------

// Lab - Filtering on NULL values

import org.apache.spark.sql.functions._
val dfNull=df.filter(col("Resourcegroup").isNull)
dfNull.count()





// COMMAND ----------

val dfNotNull=df.filter(col("Resourcegroup").isNotNull)
dfNotNull.count()

// COMMAND ----------

df.write.saveAsTable("logdata")

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM logdata
