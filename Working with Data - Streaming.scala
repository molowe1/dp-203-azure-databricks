// Databricks notebook source
// MAGIC %md
// MAGIC
// MAGIC // Lab - Streaming data using Azure Databricks

// COMMAND ----------

// MAGIC %sql
// MAGIC DELETE FROM DimCustomer

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE TABLE if not EXISTS DimCustomer(
// MAGIC     CustomerID STRING,
// MAGIC     CompanyName STRING,
// MAGIC 	SalesPerson STRING
// MAGIC )

// COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.datalakedemodp203.dfs.core.windows.net",
    "aTVPvtKPOE2ZdZm8o2uCG42KJM93qYnySCktFMb5TEjQiNbAoTf35s1Ha1ATgAg12cPbXxki7In++AStaTR/Uw==")

val path="abfss://csv@datalakedemodp203.dfs.core.windows.net/Customer/"
val checkpointPath="abfss://checkpoint@datalakedemodp203.dfs.core.windows.net/" //new container
val schemaLocation="abfss://schema@datalakedemodp203.dfs.core.windows.net/" //new container

// COMMAND ----------

// We need to have atleast one file in the location
//autoloader used to detect the schema of the loaded data - to infer the schema from the source, it needs to have some sort of location to keep track of that schema.
val dfDimCustomer=(spark.readStream.format("cloudfiles") //create a new dataframe
    .option("cloudFiles.schemaLocation", schemaLocation) 
    .option("cloudFiles.format","csv")
    .load(path))



// COMMAND ----------

val finaldfDimCustomer=dfDimCustomer.dropDuplicates("CustomerID")

// COMMAND ----------

//to take the data that we are reading from the stream and write it onto our table, our dimension customer table.
finaldfDimCustomer.writeStream.format("delta") 
.option("checkpointLocation",checkpointPath) //
.option("mergeSchema", "true")
.table("DimCustomer")

//*So it's gonna read the data in the dimension customer 
//folder in a Azure Data Lake Gen 2 storage account. 
//Let's say we've added one file, or one blob of data,
//it's taken that data and streamed it onto our table.
//So it's gonna add a checkpoint
//saying that it has already gone ahead and read this file.
//If you add another file now,
//the stream will remember this checkpoint location
//and start reading from the next file onwards.
//It just ensures that it understands the data
//that it has already read within the stream itself.
//So now that we have all of this in place,
//let me run this cell.
//And before I actually can run this cell
//let me introduce a new cell

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from DimCustomer

// COMMAND ----------

// MAGIC
// MAGIC %sql
// MAGIC SELECT CustomerID,count(CustomerID)
// MAGIC FROM DimCustomer
// MAGIC GROUP BY CustomerID
// MAGIC HAVING count(CustomerID)>1;
