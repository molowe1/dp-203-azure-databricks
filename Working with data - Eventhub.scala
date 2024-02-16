// Databricks notebook source
// MAGIC %md
// MAGIC resource link - https://learn.microsoft.com/en-us/azure/databricks/archive/azure/streaming-event-hubs

// COMMAND ----------

import org.apache.spark.eventhubs._

val eventHubConnectionString = ConnectionStringBuilder("Endpoint=sb://webeventdp203.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=J1TXt8QdRn0xgQJChQFRbKwE9rpTnDIt9+AEhNCsoA8=;EntityPath=webhub")
  .build

val eventHubConfiguration = EventHubsConf(eventHubConnectionString)  

// COMMAND ----------

var webhubDF = 
  spark.readStream
    .format("eventhubs")
    .options(eventHubConfiguration.toMap)
    .load()

//display(webhubDF)

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val recordsDF=webhubDF
.select(get_json_object(($"body").cast("string"), "$.records").alias("records"))

//display(recordsDF)

// COMMAND ----------

val maxMetrics = 30 
val jsonElements = (0 until maxMetrics)
                     .map(i => get_json_object($"records", s"$$[$i]"))

// COMMAND ----------

val  expandedDF= recordsDF
  .withColumn("records", explode(array(jsonElements: _*))) 
  .where(!isnull($"records")) 

// COMMAND ----------

//display(expandedDF)

// COMMAND ----------

val dataSchema = new StructType()
        .add("count", LongType)
        .add("total", LongType)
        .add("minimum", LongType)
        .add("maximum", LongType)
        .add("resourceId", StringType)
        .add("time", DataTypes.DateType)
        .add("metricName", StringType)
        .add("timeGrain", StringType)
        .add("average", LongType)

// COMMAND ----------

val jsondf=expandedDF.withColumn("records",from_json(col("records"),dataSchema))

val finalDF=jsondf.select(col("records.*"))

// COMMAND ----------

display(finalDF)
