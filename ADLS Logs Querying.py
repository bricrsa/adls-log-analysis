# Databricks notebook source
# MAGIC %md ### This notebook shows how to query classic logs using Azure Data Lake Storage.
# MAGIC 
# MAGIC ### Step 1: Mount the $logs container

# COMMAND ----------

dbutils.widgets.text("accountName", "", "Account Name:")

# COMMAND ----------


configs = {
  "fs.azure.account.auth.type": "CustomAccessToken",
  "fs.azure.account.custom.token.provider.class":   spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
}
mountPoint = "/mnt/adls-logs"

if mountPoint in [m.mountPoint for m in dbutils.fs.mounts()]:
  dbutils.fs.unmount(mountPoint)

dbutils.fs.mount(
  source = "abfss://$logs@" + dbutils.widgets.get("accountName") + ".dfs.core.windows.net/blob/",
  mount_point = mountPoint,
  extra_configs = configs)

# COMMAND ----------

# MAGIC %md ### Step 2: Define a table structure pointing at the logs
# MAGIC 
# MAGIC Define a Spark SQL table (RawAdlsLogs) that allows us to access the data written to the $logs container.
# MAGIC 
# MAGIC Note: We include the non-CSV delimiters and partition structure in this DDL statement.
# MAGIC 
# MAGIC We also declare a view (AdlsLogs) that includes some computed columns. The `PartitionedRequestDate` computed column allows queries to filter rows using Timestamp arithmetic and still perform efficient partition pruning.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC /* Only run this cell when you want to start over. Each time the table is dropped & re-created you will need to re-discovery all of the partitions */
# MAGIC /*DROP TABLE IF EXISTS RawAdlsLogs;*/
# MAGIC 
# MAGIC CREATE TABLE RawAdlsLogs (
# MAGIC     Version  STRING,
# MAGIC     RequestStartTime  TIMESTAMP,
# MAGIC     OperationType  STRING,
# MAGIC     RequestStatus  STRING, 
# MAGIC     StatusCode INT,
# MAGIC     EndToEndLatencyMS LONG,
# MAGIC     ServerLatencyMS LONG,
# MAGIC     AuthenticationType  STRING,
# MAGIC     RequesterAccountName  STRING,
# MAGIC     OwnerAccountName  STRING,
# MAGIC     ServiceType  STRING,
# MAGIC     RequestedURL  STRING,
# MAGIC     RequestedObjectKey  STRING,
# MAGIC     RequestedIDHeader  STRING,
# MAGIC     OperationCount INT,
# MAGIC     RequesterIPAddress  STRING,
# MAGIC     RequestVersionHeader  STRING,
# MAGIC     RequestHeaderSize LONG,
# MAGIC     RequestPacketSize LONG,
# MAGIC     ResponseHeaderSize LONG,
# MAGIC     ResponsePacketSize LONG,
# MAGIC     ResponseContentLength LONG,
# MAGIC     RequestMD5  STRING,
# MAGIC     ServerMD5  STRING,
# MAGIC     ETagIdentifier  STRING,
# MAGIC     LastModifiedTime  STRING,
# MAGIC     ConditionsUsed  STRING,
# MAGIC     UserAgentHeader  STRING,
# MAGIC     RefererHeader  STRING,
# MAGIC     ClientRequestId  STRING,
# MAGIC     UserObjectId STRING,
# MAGIC     TenantId STRING,
# MAGIC     ApplicationId STRING,
# MAGIC     ResourceID STRING,
# MAGIC     Issuer STRING,
# MAGIC     UserPrincipalName STRING,
# MAGIC     Reserved STRING,
# MAGIC     AuthorizationDetail STRING,
# MAGIC     Year INT, 
# MAGIC     Month INT, 
# MAGIC     Day INT, 
# MAGIC     Hour INT)
# MAGIC USING CSV
# MAGIC OPTIONS (sep=';')
# MAGIC PARTITIONED BY (Year, Month, Day, Hour)
# MAGIC LOCATION '/mnt/adls-logs/';

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP VIEW IF EXISTS AdlsLogs;
# MAGIC 
# MAGIC CREATE VIEW AdlsLogs
# MAGIC AS
# MAGIC SELECT 
# MAGIC   *,
# MAGIC   to_timestamp(concat(string(Year), "-", string(Month), "-", string(Day), " ", string(Hour / 100)), "yyyy-MM-dd HH") as PartitionedRequestDate,
# MAGIC   case 
# MAGIC     when isnotnull(UserPrincipalName) then UserPrincipalName
# MAGIC     when isnotnull(UserObjectId) then UserObjectId
# MAGIC     else AuthenticationType
# MAGIC   end as Authenticated  
# MAGIC FROM RawAdlsLogs

# COMMAND ----------

# MAGIC %md ###Step 3: Discover partition directories
# MAGIC 
# MAGIC The directory structure of the logs does permit partition declarations (as seen in the DDL for AdlsLogs), but it 
# MAGIC cannot work with the normal partition discovery mechanism. Therefore the next cell must be periodically run in order
# MAGIC to ensure that all partitions are registered and data accessible.

# COMMAND ----------

def discover_partitions(basePath, partitionInfo, parts, partIdx, existingPartitions):
  if partIdx == len(parts):
      if partitionInfo not in existingPartitions:
        print(",".join([key+'='+value for (key,value) in partitionInfo.items()]))
        spark.sql("ALTER TABLE RawAdlsLogs ADD IF NOT EXISTS PARTITION (" + ",".join([key+'='+value for (key,value) in partitionInfo.items()]) + ") LOCATION '" + basePath + "'")
  else:
    items = dbutils.fs.ls(basePath)
    for item in items:
      if item.isDir():
        newPartitionInfo = {**partitionInfo, **{parts[partIdx]: item.name.rstrip('/')}}
        discover_partitions(item.path, newPartitionInfo, parts, partIdx + 1, existingPartitions)
      
parts = spark.sql('SHOW PARTITIONS RawAdlsLogs')\
  .rdd.map(lambda row: {part.split('=')[0]:part.split('=')[1] for (part) in row[0].split('/')})\
  .collect()
discover_partitions('/mnt/adls-logs', {}, ['Year', 'Month', 'Day', 'Hour'], 0, parts)


# COMMAND ----------

# MAGIC %md ### Step 4: Query the logs along partition lines 
# MAGIC We can query this table using Spark SQL. The WHERE clause should include as much as the partition information as possible to avoid
# MAGIC full directory scans, which will cause the query to run slowly.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select count(*) from AdlsLogs
# MAGIC where year = 2020 and month = 1

# COMMAND ----------

# MAGIC %md Date arithmatic can be applied to the calculated column `PartitionedRequestDate` such that partition pruning is effectively applied:

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select count(*), min(RequestStartTime), max(RequestStartTime) 
# MAGIC from AdlsLogs 
# MAGIC where PartitionedRequestDate >= date_sub(current_timestamp(), 7)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT StatusCode, count(*) AS Count
# MAGIC FROM AdlsLogs 
# MAGIC WHERE date(PartitionedRequestDate) = '2020-03-27'
# MAGIC GROUP BY StatusCode
# MAGIC ORDER BY StatusCode

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT 
# MAGIC   date(RequestStartTime) as RequestDate,
# MAGIC   Authenticated,
# MAGIC   count(*) as Count
# MAGIC FROM AdlsLogs 
# MAGIC WHERE PartitionedRequestDate >= date_sub(current_timestamp(), 183)
# MAGIC GROUP BY RequestDate, Authenticated
# MAGIC ORDER BY RequestDate, Authenticated

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT 
# MAGIC   date(RequestStartTime) as RequestDate,
# MAGIC   Authenticated,
# MAGIC   OperationType,
# MAGIC   count(*) as Count
# MAGIC FROM AdlsLogs 
# MAGIC WHERE PartitionedRequestDate >= date_sub(current_timestamp(), 7)
# MAGIC GROUP BY RequestDate, Authenticated, OperationType
# MAGIC ORDER BY RequestDate, Authenticated, OperationType