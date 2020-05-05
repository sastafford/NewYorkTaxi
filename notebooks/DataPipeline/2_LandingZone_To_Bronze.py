# Databricks notebook source
# MAGIC %md 
# MAGIC # LandingZone To Bronze
# MAGIC 
# MAGIC This notebook monitors the landing zone for new data.  Upon receiving the data, the data is transformed to Delta Lake format and streamed out to a cloud storage as a bronze table

# COMMAND ----------

from datetime import datetime
from dateutil import parser

from pyspark.sql.types import IntegerType, DoubleType
from pyspark.sql.functions import col, lit


# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite = true;
# MAGIC set spark.databricks.delta.properties.defaults.autoOptimize.autoCompact = true;

# COMMAND ----------

# Define the schema using a DDL-formatted string.
yellowSchema = "vendorID string, \
tpepPickupDateTime timestamp, \
tpepDropoffDateTime timestamp, \
passengerCount integer, \
tripDistance double, \
puLocationId string, \
doLocationId string, \
startLon double, \
startLat double, \
endLon double, \
endLat double, \
rateCodeId integer, \
storeAndFwdFlag string, \
paymentType string, \
fareAmount double, \
extra double, \
mtaTax double, \
improvementSurcharge string, \
tipAmount double, \
tollsAmount double, \
totalAmount double, \
puYear integer, \
puMonth integer"

# COMMAND ----------

greenSchema = "vendorID integer, \
lpepPickupDatetime timestamp, \
lpepDropoffDatetime timestamp, \
passengerCount integer, \
tripDistance double, \
puLocationId string, \
doLocationId string, \
pickupLongitude double, \
pickupLatitude double, \
dropoffLongitude double, \
dropoffLatitude double, \
rateCodeID integer, \
storeAndFwdFlag string, \
paymentType integer, \
fareAmount double, \
extra double, \
mtaTax double, \
improvementSurcharge string, \
tipAmount double, \
tollsAmount double, \
ehailFee double, \
totalAmount double, \
tripType integer, \
puYear integer, \
puMonth integer"

# COMMAND ----------

fhvSchema="dispatchBaseNum string, \
pickupDateTime timestamp, \
dropOffDateTime timestamp, \
puLocationId string, \
doLocationId string, \
srFlag string, \
puYear integer, \
puMonth integer"

# COMMAND ----------

# DBTITLE 1,Read in streaming data from landing zone
yellowRawDF = spark.readStream.format("csv").schema(yellowSchema).load("/mnt/xferblob/ingest/nyc-taxi-yellow-raw/")
greenRawDF = spark.readStream.format("csv").schema(greenSchema).load("/mnt/xferblob/ingest/nyc-taxi-green-raw")
fhvRawDF = spark.readStream.format("csv").schema(fhvSchema).load("/mnt/xferblob/ingest/nyc-taxi-fhv-raw")

# COMMAND ----------

# DBTITLE 1,Append streaming data to Bronze Delta Lake table using checkpoints
yellowRawDF.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("mergeSchema", "true") \
  .option("checkpointLocation", "/mnt/xferblob/bronze/checkpoint/nyc-taxi-yellow-bronze/") \
  .start("/mnt/xferblob/bronze/delta/nyc-taxi-yellow-bronze")

# COMMAND ----------

greenRawDF.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("mergeSchema", "true") \
  .option("checkpointLocation", "/mnt/xferblob/bronze/checkpoint/nyc-taxi-green-bronze/") \
  .start("/mnt/xferblob/bronze/delta/nyc-taxi-green-bronze")

# COMMAND ----------

fhvRawDF.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("mergeSchema", "true") \
  .option("checkpointLocation", "/mnt/xferblob/bronze/checkpoint/nyc-taxi-fhv-bronze/") \
  .start("/mnt/xferblob/bronze/delta/nyc-taxi-fhv-bronze")

# COMMAND ----------

# DBTITLE 1,Stop data streams
# UNCOMMENT WHEN WANT TO STOP STREAMS - Iterates over all active streams - Can also press Stop Execution at the top.
#for s in spark.streams.active:  # Iterate over all active streams
#    print("Stopping "+s.name)   # A little extra feedback
#    s.stop()                    # Stop the stream       