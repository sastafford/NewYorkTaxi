# Databricks notebook source
# MAGIC %sql
# MAGIC 
# MAGIC CREATE DATABASE IF NOT EXISTS nyc_taxi

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC USE nyc_taxi

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE taxi_zone_lookup
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/xferblob/metadata/nyc-taxi-zone-lookup-delta/'

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC CREATE TABLE taxi_zone
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/xferblob/metadata/nyc-taxi-zones-delta/'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE nyc_taxi_gold
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/xferblob/gold/delta/nyc-taxi-gold'