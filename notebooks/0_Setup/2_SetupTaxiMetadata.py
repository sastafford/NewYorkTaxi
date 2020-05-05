# Databricks notebook source
# MAGIC %fs
# MAGIC ls /shared/nyctaxi

# COMMAND ----------

deltaDataPath = "/mnt/xferblob/metadata/nyc-taxi-zone-lookup-delta/"

(spark.read.format("csv")
 .option("header", "true")
 .option("inferSchema", "true") 
 .load('/shared/nyctaxi/taxi_zone_lookup.csv')
 .write
 .mode("overwrite")
 .format("delta")
 .save(deltaDataPath))

# COMMAND ----------

taxiZonesDirectory = "/mnt/xferblob/metadata/nyc-taxi-zones-delta/"

(spark.read.format("csv")
 .option("header", "true")
 .option("inferSchema", "true") 
 .load('/shared/nyctaxi/nyc_taxi_zones.wkt.csv')
 .write
 .mode("overwrite")
 .format("delta")
 .save(taxiZonesDirectory))