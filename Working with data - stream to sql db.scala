// Databricks notebook source
// MAGIC %md
// MAGIC code reference for sql connector - https://learn.microsoft.com/en-us/azure/hdinsight/spark/apache-spark-connect-to-sql-database

// COMMAND ----------

//what is the purpose of these libraries
import java.util.Properties
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import java.sql.{Connection,DriverManager,ResultSet}

// COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.datalakedemodp203.dfs.core.windows.net",
    dbutils.secrets.get(scope="datalakedemodp203_key", key="datalakedemodp203-secret"))

// COMMAND ----------

// MAGIC %python
// MAGIC dbutils.fs.ls("/mnt/dp203_datalake")
// MAGIC

// COMMAND ----------

// MAGIC %python
// MAGIC dbutils.fs.cp("/mnt/dp203_datalake/draft.csv","/mnt/dp203_datalake/draft.csv")
// MAGIC

// COMMAND ----------

//val userSchema = spark.read.option("basePath", "header", "true").csv("abfss://csv@datalakedemodp203.dfs.core.windows.net/Customer/Customer01.csv").schema
val userSchema = spark.read
       //.option("basePath", "/mnt/dp203_datalake")
       .option("header", "true")
       .option("inferSchema", "true")
       .csv("/mnt/dp203_datalake/Customer/Customer02.csv")
       .schema
//val readStreamDf = spark.readStream.format("csv").schema(userSchema).load("abfss://csv@datalakedemodp203.dfs.core.windows.net/Customer/Customer01.csv")
val readStreamDf = spark.readStream
       .format("csv")
       //.option("basePath", "/mnt/dp203_datalake")
       //.option("skipRows",1)
       .schema(userSchema)
       .load("/mnt/dp203_datalake/Customer")


// COMMAND ----------

readStreamDf.printSchema


// COMMAND ----------

val WriteToSQLQuery  = readStreamDf.writeStream
    //.option("basePath", "/mnt/dp203_datalake")
    .foreach(new ForeachWriter[Row] {
    var connection:java.sql.Connection = _
    var statement:java.sql.Statement = _
    
    // Declare the values for your database

    val jdbcUsername = "Dp203admin"
    val jdbcPassword = dbutils.secrets.get(scope="datalakedemodp203_key", key="dp-password")
    val jdbcHostname = "dp203section4.database.windows.net" //typically, this is in the form or servername.database.windows.net
    val jdbcPort = 1433
    val jdbcDatabase ="adf-db"

    val driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    val jdbc_url = s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=60;"

    def open(partitionId: Long, version: Long):Boolean = {
    Class.forName(driver)
    connection = DriverManager.getConnection(jdbc_url, jdbcUsername, jdbcPassword)
    statement = connection.createStatement
    true
    }

    def process(value: Row): Unit = {
    val CustomerID  = value(0)
    val NameStyle = value(1)
    val Title = value(2)
    val FirstName = value(3)
    val MiddleName = value(4)
    val LastName = value(5)
    val Suffix = value(6)
    val CompanyName = value(7)  
    val SalesPerson = value(8)  
    val EmailAddress = value(9)  
    val Phone = value(10)  
    val PasswordHash = value(11)  
    val PasswordSalt = value(12)  
    val rowguid = value(13)  

    val valueStr = "'" + CustomerID + "','" + NameStyle + "','" + Title + "','" + FirstName + "','" + MiddleName + "','" + LastName + "','" + Suffix + "','" + CompanyName + "','" + SalesPerson + "','" + EmailAddress + "','" + Phone + "','" + PasswordHash + "','" + PasswordSalt + "','" + rowguid + "'"
    statement.execute("INSERT INTO " + "dbo.customer_table" + " VALUES (" + valueStr + ")")
    }

    def close(errorOrNull: Throwable): Unit = {
    connection.close
    }
    })

var streamingQuery = WriteToSQLQuery.start()

// COMMAND ----------

val jdbcUsername = "Dp203admin"
val jdbcPassword = dbutils.secrets.get(scope="datalakedemodp203_key", key="dp-password")
val jdbcHostname = "dp203section4.database.windows.net" //typically, this is in the form or servername.database.windows.net
val jdbcPort = 1433
val jdbcDatabase ="adf-db"
val jdbc_url_2 = s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=60;"
val connectionProperties = new Properties()
connectionProperties.put("user", s"${jdbcUsername}")
connectionProperties.put("password", s"${jdbcPassword}")
val sqlTableDF = spark.read.jdbc(jdbc_url_2, "Dbo.customer_table", connectionProperties)

// COMMAND ----------

sqlTableDF.printSchema

// COMMAND ----------

sqlTableDF.show(5)

// COMMAND ----------

display(sqlTableDF)
