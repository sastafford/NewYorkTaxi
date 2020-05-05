# Databricks notebook source
from pyspark.sql.functions import col, window, max, date_format

# COMMAND ----------

# MAGIC %md ###Setup the Raw Data CSV files

# COMMAND ----------

yellowRawDirectory = "/mnt/xferblob/ingest/nyc-taxi-yellow-raw/"
greenRawDirectory = "/mnt/xferblob/ingest/nyc-taxi-green-raw/"
fhvRawDirectory = "/mnt/xferblob/ingest/nyc-taxi-fhv-raw/"

# COMMAND ----------

# MAGIC %md ###Setup the Bronze Tables:  Yellow, Green, and For-Hire-Vehicle

# COMMAND ----------

yellowDeltaDirectory = "/mnt/xferblob/bronze/delta/nyc-taxi-yellow-bronze/"
greenDeltaDirectory = "/mnt/xferblob/bronze/delta/nyc-taxi-green-bronze/"
fhvDeltaDirectory = "/mnt/xferblob/bronze/delta/nyc-taxi-fhv-bronze/"

# COMMAND ----------

# MAGIC %md ###Setup the Silver Table

# COMMAND ----------

silverDeltaDirectory = "/mnt/xferblob/silver/delta/nyc-taxi-silver"

# COMMAND ----------

# MAGIC %md ###Read in the Gold Table of Data

# COMMAND ----------

goldDeltaDirectory = "/mnt/xferblob/gold/delta/nyc-taxi-gold"
goldDF = spark.readStream.format("delta").load(goldDeltaDirectory)
#display(goldDF.where(df.tip_alert == True))

# COMMAND ----------

# MAGIC %md ###Get the current date being processed

# COMMAND ----------

dfMaxDate = goldDF.select("*").agg(max("pickupDate").alias("maxPickupDate"))
dfPrettyMaxDate = dfMaxDate.withColumn("maxPickupDate", date_format(col("maxPickupDate"), "MMM-dd-yyyy"))
display(dfPrettyMaxDate)

# COMMAND ----------

# MAGIC %md ###Break up the data into 1 hour windows based on the Pick Up Date and Time
# MAGIC - Select any data that you may want to plot

# COMMAND ----------

windowDF = goldDF.select(col("vendorID"), col("pickupZone"), col("tripDistance"), col("totalAmount"), col("pickupDate"), col("pickupDateTime"), col("sourceDataset"), window(col("pickupDateTime").cast("timestamp"), "60 minutes"))

# COMMAND ----------

# MAGIC %md ###Total the trips per hour

# COMMAND ----------

hourlyCountDF = windowDF.groupBy((date_format(col("window.end"), "MM-dd HH:mm").alias("endLabel")), "window.end", "sourceDataset").count()

# COMMAND ----------

#Create an in-memory table to query via SQL
#However, does not update the table with stream updates

# query = (
#   hourlyCountDF
#     .writeStream
#     .format("memory")              # memory = store in-memory table (for testing only in Spark 2.0)
#     .queryName("hourly_count")     # counts = name of the in-memory table
#     .outputMode("complete")        # complete = all the counts should be in the table
#     .start()
# )

# COMMAND ----------

# MAGIC %md ###Total the trips per day

# COMMAND ----------

dailyCountDF = goldDF.groupBy("pickupDate", "sourceDataset").count()

# COMMAND ----------

# MAGIC %md ###Plot the number of trips
# MAGIC - First plot is the trips in the last 24 hours
# MAGIC - Second plot is the trips over a period of days

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Using an in-memory table only which does not seem to update with the stream
# MAGIC -- select date_format(end, 'MMM-dd HH:mm') as time, count from hourly_count order by end desc limit 24

# COMMAND ----------

display(hourlyCountDF.orderBy('endLabel', ascending=False).limit(24).sort("end"))

# COMMAND ----------

display(dailyCountDF.orderBy("pickupDate", ascending=False).limit(7).sort("pickupDate"))

# COMMAND ----------

# MAGIC %md ###Show the history of changes to the Detla Tables
# MAGIC - Use the last column, operation metrics, to see the number of incoming records

# COMMAND ----------

from delta.tables import *

# COMMAND ----------

yellowDeltaTable = DeltaTable.forPath(spark, yellowDeltaDirectory)
fullYellowHistoryDF = yellowDeltaTable.history()    # get the full history of the table

display(fullYellowHistoryDF)

# COMMAND ----------

greenDeltaTable = DeltaTable.forPath(spark, greenDeltaDirectory)
fullGreenHistoryDF = greenDeltaTable.history()    # get the full history of the table

display(fullGreenHistoryDF)

# COMMAND ----------

fhvDeltaTable = DeltaTable.forPath(spark, fhvDeltaDirectory)
fullFhvHistoryDF = fhvDeltaTable.history()    # get the full history of the table

display(fullFhvHistoryDF)

# COMMAND ----------

silverDeltaTable = DeltaTable.forPath(spark, silverDeltaDirectory)
fullSilverHistoryDF = silverDeltaTable.history()    # get the full history of the table

display(fullSilverHistoryDF)

# COMMAND ----------

goldDeltaTable = DeltaTable.forPath(spark, goldDeltaDirectory)
fullGoldHistoryDF = goldDeltaTable.history()    # get the full history of the table

display(fullGoldHistoryDF)

# COMMAND ----------

# MAGIC %md ###Stop the streaming in of data
# MAGIC - UNCOMMENT WHEN WANT TO STOP STREAMS - Iterates over all active streams - Can also press Stop Execution at the top.

# COMMAND ----------

# UNCOMMENT WHEN WANT TO STOP STREAMS - Iterates over all active streams - Can also press Stop Execution at the top.
# for s in spark.streams.active:  # Iterate over all active streams
#    print("Stopping "+s.name)   # A little extra feedback
#    s.stop()                    # Stop the stream       

# COMMAND ----------

yellowCountDF = yellowBronzeDF.select(col("tpepPickupDateTime"), window(col("tpepPickupDateTime").cast("timestamp"), "10 minutes"))

# COMMAND ----------

plotYellowDF = yellowCountDF.groupBy((date_format(col("window.end"), "MM-dd HH:mm").alias("endLabel")), "window.end").count()

# COMMAND ----------

greenCountDF = greenBronzeDF.select(col("lpepPickupDateTime"), window(col("lpepPickupDateTime").cast("timestamp"), "10 minutes"))

# COMMAND ----------

plotGreenDF = greenCountDF.groupBy((date_format(col("window.end"), "MM-dd HH:mm").alias("endLabel")), "window.end").count()

# COMMAND ----------

fhvCountDF = fhvBronzeDF.select(col("pickupDateTime"), window(col("pickupDateTime").cast("timestamp"), "10 minutes"))

# COMMAND ----------

plotFhvDF = fhvCountDF.groupBy((date_format(col("window.end"), "MM-dd HH:mm").alias("endLabel")), "window.end").count()

# COMMAND ----------

#display(plotYellowDF.orderBy('endLabel', ascending=False).limit(36).sort("end"))