-- Databricks notebook source
drop table DimCustomer

-- COMMAND ----------

CREATE TABLE DimCustomer(
    CustomerID INT,
    CompanyName STRING,
	SalesPerson STRING
)

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC spark.conf.set(
-- MAGIC     "fs.azure.account.key.datalakedemodp203.dfs.core.windows.net",
-- MAGIC     dbutils.secrets.get(scope="datalakedemodp203_key", key="datalakedemodp203-secret"))
-- MAGIC
-- MAGIC val path="abfss://csv@datalakedemodp203.dfs.core.windows.net/Customer/"
-- MAGIC val checkpointPath="abfss://checkpoint@datalakedemodp203.dfs.core.windows.net/" //new container
-- MAGIC

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC import org.apache.spark.sql.types._
-- MAGIC import org.apache.spark.sql.functions._
-- MAGIC
-- MAGIC val dataSchema = StructType(Array(    
-- MAGIC     StructField("CustomerID", IntegerType, true),
-- MAGIC     StructField("CompanyName", StringType, true),
-- MAGIC     StructField("SalesPerson", StringType, true)))

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC val dfDimCustomer=(spark.readStream.format("cloudfiles")
-- MAGIC     .schema(dataSchema)    
-- MAGIC     .option("cloudFiles.format","csv")
-- MAGIC     .option("header",true)
-- MAGIC     .load(path))

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC val finaldfDimCustomer=dfDimCustomer.dropDuplicates("CustomerID")
-- MAGIC

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC finaldfDimCustomer.writeStream.format("delta")
-- MAGIC .option("checkpointLocation",checkpointPath)
-- MAGIC .option("mergeSchema", "true")
-- MAGIC .table("DimCustomer")

-- COMMAND ----------

select * from DimCustomer

-- COMMAND ----------

SELECT CustomerID,count(CustomerID)
FROM DimCustomer
GROUP BY CustomerID
HAVING count(CustomerID)>1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC --looking up data table versions

-- COMMAND ----------

DESCRIBE HISTORY DimCustomer

-- COMMAND ----------

SELECT * FROM dimcustomer VERSION AS OF 1
