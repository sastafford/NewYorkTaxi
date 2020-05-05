# Databricks notebook source
# MAGIC %md 
# MAGIC # Bronze To Silver
# MAGIC 
# MAGIC This notebook streams data from the Bronze tables.  Upon receiving the data, the data is normalized such that column names match and the same number of columns are in each row ingested.  All data is given a new column with its ingest source of either "yellow" or "green" or "fhv".  The yellow and green data is then merged into one table and appended to a silver delta table

# COMMAND ----------

from pyspark.sql.types import IntegerType, DoubleType
from pyspark.sql.functions import col, lit, to_date

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite = true;
# MAGIC set spark.databricks.delta.properties.defaults.autoOptimize.autoCompact = true;

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Read in the Bronze Delta Lake tables and normalize the data
# MAGIC - Read in the bronze tables
# MAGIC - Rename columns so similar data has the same column name
# MAGIC - Cast columns to the best datatype
# MAGIC - Drop columns from tables that have unique data from the three different bronze tables (i.e. green's tripType)
# MAGIC - Add columns to tables where data can be inferred (i.e. fees only charged for green)
# MAGIC - Add a column to identify the source of the data (i.e. yellow)

# COMMAND ----------

yellowBronzeDF = spark.readStream.format("delta").load("/mnt/xferblob/bronze/delta/nyc-taxi-yellow-bronze/") \
  .withColumnRenamed("tpepPickupDateTime", "pickupDateTime") \
  .withColumn("pickupDate", to_date(col("pickupDateTime"))) \
  .withColumnRenamed("tpepDropoffDateTime", "dropoffDateTime") \
  .withColumn("dropoffDate", to_date(col("dropoffDateTime"))) \
  .withColumn("pickupLocationId", col("puLocationId").cast(IntegerType())) \
  .withColumn("dropoffLocationId", col("doLocationId").cast(IntegerType())) \
  .withColumn("improvementSurcharge", col("improvementSurcharge").cast(DoubleType())) \
  .withColumn("paymentType", col("paymentType").cast(IntegerType())) \
  .withColumnRenamed("puYear", "pickupYear") \
  .withColumnRenamed("puMonth", "pickupMonth") \
  .withColumn("sourceDataset", lit("yellow")) \
  .withColumn("ehailFee", lit(0.0))

# COMMAND ----------

greenBronzeDF = spark.readStream.format("delta").load("/mnt/xferblob/bronze/delta/nyc-taxi-green-bronze") \
  .withColumnRenamed("lpepPickupDatetime", "pickupDateTime") \
  .withColumn("pickupDate", to_date(col("pickupDateTime"))) \
  .withColumnRenamed("lpepDropoffDatetime", "dropoffDateTime") \
  .withColumn("dropoffDate", to_date(col("dropoffDateTime"))) \
  .withColumnRenamed("pickupLongitude", "startLon") \
  .withColumnRenamed("pickupLatitude", "startLat") \
  .withColumnRenamed("dropoffLongitude", "endLon") \
  .withColumnRenamed("dropoffLatitude", "endLat") \
  .withColumn("pickupLocationId", col("puLocationId").cast(IntegerType())) \
  .withColumn("dropoffLocationId", col("doLocationId").cast(IntegerType())) \
  .withColumn("improvementSurcharge", col("improvementSurcharge").cast(DoubleType())) \
  .withColumnRenamed("puYear", "pickupYear") \
  .withColumnRenamed("puMonth", "pickupMonth") \
  .withColumn("sourceDataset", lit("green"))

# COMMAND ----------

fhvBronzeDF = spark.readStream.format("delta").load("/mnt/xferblob/bronze/delta/nyc-taxi-fhv-bronze") \
  .withColumn("pickupDate", to_date(col("pickupDateTime"))) \
  .withColumnRenamed("dropOffDateTime", "dropoffDateTime") \
  .withColumn("dropoffDate", to_date(col("dropoffDateTime"))) \
  .withColumn("pickupLocationId", col("puLocationId").cast(IntegerType())) \
  .withColumn("dropoffLocationId", col("doLocationId").cast(IntegerType())) \
  .withColumnRenamed("puYear", "pickupYear") \
  .withColumnRenamed("puMonth", "pickupMonth") \
  .withColumn("sourceDataset", lit("fhv"))

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Append the data to silver table
# MAGIC - Merge the tables together if the tables have the same schema after processing
# MAGIC - Partition and Optimize the data and append it to the silver Delta Lake table

# COMMAND ----------

taxiRidesDf = yellowBronzeDF.select(*['vendorID', 'pickupDateTime', 'pickupDate', 'dropoffDateTime', 'dropoffDate', 'pickupLocationId', 'dropoffLocationId', 'passengerCount', 'tripDistance', 'rateCodeId', 'storeAndFwdFlag', 'paymentType', 'fareAmount', 'extra', 'mtaTax', 'improvementSurcharge', 'tipAmount', 'tollsAmount', 'totalAmount', 'pickupYear', 'pickupMonth', 'sourceDataset', 'ehailFee']) \
.union(greenBronzeDF.select(*['vendorID', 'pickupDateTime', 'pickupDate', 'dropoffDateTime', 'dropoffDate',  'pickupLocationId', 'dropoffLocationId', 'passengerCount', 'tripDistance', 'rateCodeId', 'storeAndFwdFlag', 'paymentType', 'fareAmount', 'extra', 'mtaTax', 'improvementSurcharge', 'tipAmount', 'tollsAmount', 'totalAmount', 'pickupYear', 'pickupMonth', 'sourceDataset', 'ehailFee'])) 
          
taxiRidesDf.createOrReplaceTempView("taxiRides")

# COMMAND ----------

taxiZoneLookupDf = spark.read.format("delta").load("/mnt/xferblob/metadata/nyc-taxi-zone-lookup-delta")
taxiZoneLookupDf.createOrReplaceTempView("taxi_zone_lookup")

# COMMAND ----------


silverDf = sqlContext.sql(" \
  SELECT taxis.*, taxi_zone_lookup.Borough AS DropoffBurrough, taxi_zone_lookup.Zone AS DropoffZone \
  FROM ( \
    SELECT taxiRides.*, taxi_zone_lookup.Borough AS PickupBurrough, taxi_zone_lookup.Zone AS PickupZone \
    FROM taxiRides \
    LEFT JOIN taxi_zone_lookup \
    ON taxiRides.pickupLocationId = taxi_zone_lookup.LocationID \
  ) as taxis \
  LEFT JOIN taxi_zone_lookup \
    ON taxis.dropoffLocationId = taxi_zone_lookup.LocationID \
")

# COMMAND ----------

silverDf.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("mergeSchema", "true") \
  .option("checkpointLocation", "/mnt/xferblob/silver/checkpoint/nyc-taxi-silver/") \
  .partitionBy("pickupDate") \
  .start("/mnt/xferblob/silver/delta/nyc-taxi-silver")

# COMMAND ----------

# DBTITLE 1,Stop data streams
# UNCOMMENT WHEN WANT TO STOP STREAMS - Iterates over all active streams - Can also press Stop Execution at the top.
#for s in spark.streams.active:  # Iterate over all active streams
#    print("Stopping "+s.name)   # A little extra feedback
#    s.stop()                    # Stop the stream       