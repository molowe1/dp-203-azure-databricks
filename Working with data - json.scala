// Databricks notebook source
// Lab - JSON-based files

val dfjson = spark.read.format("json").load("/FileStore/json/customer_arr.json")



// COMMAND ----------

// Here you will see the array elements are being shown as one data item
display(dfjson)



// COMMAND ----------

import org.apache.spark.sql.functions.{col, explode}
// Use the explode function

val newjson=dfjson.select(col("customerid"),col("customername"),col("registered"),explode(col("courses")))

display(newjson)



// COMMAND ----------

val dfobj = spark.read.format("json").load("/FileStore/json/customer_obj.json")
// Here we are showing how to access elements of a nested JSON object
display(dfobj.select(col("customerid"),col("customername"),col("registered"),(col("details.city")),(col("details.mobile")),explode(col("courses")).alias("Courses")))


// COMMAND ----------

import org.apache.spark.sql.functions._
val dfjson = spark.read.format("json")
.option("multiline","true")
.load("/FileStore/json/customer01.json")
display(dfjson)
 


// COMMAND ----------

val customerjson=dfjson.select(explode(col("Customers")).alias("Customers"))
display(customerjson)
 
 


// COMMAND ----------

val coursesjson=customerjson.select(col("Customers.customerid").alias("CustomerId"),col("Customers.customername").alias("CustomerName"),
explode(col("Customers.courses")).alias("Courses"))
 
display(coursesjson)
