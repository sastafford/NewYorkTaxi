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

# MAGIC %md ###Setup and Read in the Gold Table of Data

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

windowDF = goldDF.select(col("vendorID"), col("pickupZone"), col("tripDistance"), col("totalAmount"), col("tipAmount"), col("tip_alert"), col("pickupDate"), col("pickupDateTime"), col("sourceDataset"), window(col("pickupDateTime").cast("timestamp"), "60 minutes"))

# COMMAND ----------

# MAGIC %md #####Total the trips per hour

# COMMAND ----------

hourlyCountDF = windowDF.groupBy(date_format(col("window.end"), "MM-dd HH:mm").alias("endLabel")).count()

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

# MAGIC %md #####Total the trips per day

# COMMAND ----------

dailyCountDF = goldDF.groupBy("tip_alert", "pickupZone").count()

# COMMAND ----------

# MAGIC %md ###Plot the trips
# MAGIC - First plot is the trips in the last 24 hours
# MAGIC - Second plot is the count of trips per pickupZone

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Using an in-memory table only which does not seem to update with the stream
# MAGIC -- select date_format(end, 'MMM-dd HH:mm') as time, count from hourly_count order by end desc limit 24

# COMMAND ----------

display(hourlyCountDF.orderBy('endLabel', ascending=False).limit(24).sort("endLabel"))

# COMMAND ----------

# MAGIC %md ###Show the total number of rides per day and if they triggered a tip alert

# COMMAND ----------

display(dailyCountDF.orderBy('count', ascending=False))

# COMMAND ----------

#from pyspark.sql.functions import first
#pivotDF = dailyCountDF.groupBy("pickupZone").pivot("tip_alert").agg(first('count'))

# COMMAND ----------

# MAGIC %md #Use the Delta Lake history to view the changes to the tables over time
# MAGIC - Use the last column, operation metrics, to see the number of incoming records

# COMMAND ----------

from delta.tables import *
from pyspark.sql.types import IntegerType

# COMMAND ----------

def getStatus(deltaHistoryDF) :
  if (deltaHistoryDF.filter(deltaHistoryDF.operation.like('%STREAMING%')).orderBy("timestamp", ascending=False).limit(1).select('numOutputRows').collect()[0][0] > 0) :
     return 'green'
  else :
    return 'red'

# COMMAND ----------

# MAGIC %md ##Look at the Bronze Table for the Yellow Dataset

# COMMAND ----------

yellowDeltaTable = DeltaTable.forPath(spark, yellowDeltaDirectory)
fullYellowHistoryDF = yellowDeltaTable.history()    # get the full history of the table

statsYellowHistoryDF = fullYellowHistoryDF.select("version", "timestamp", "operation", col("operationMetrics.numOutputRows").cast(IntegerType()))

displayHTML('<b>Bronze Yellow Table Status</b><br><svg width="100" height="100">\
   <circle cx="50" cy="50" r="40" stroke="green" stroke-width="4" fill='+getStatus(statsYellowHistoryDF)+' />\
   Sorry, your browser does not support inline SVG.\
</svg>')

# COMMAND ----------

#display(fullYellowHistoryDF)

# COMMAND ----------

display(statsYellowHistoryDF)

# COMMAND ----------

# MAGIC %md #####A view of the Delta Lake Optimze function running periodically

# COMMAND ----------

display(statsYellowHistoryDF.filter(statsYellowHistoryDF.operation.like('%OPTIMIZE%')).withColumn("prettyTimestamp", date_format(col("timestamp"), "MMM-dd HH:mm")).sort("timestamp"))

# COMMAND ----------

# MAGIC %md #####Show the number of rows being added to the table over time

# COMMAND ----------

display(statsYellowHistoryDF.filter(statsYellowHistoryDF.operation.like('%STREAMING%')).withColumn("prettyTimestamp", date_format(col("timestamp"), "MMM-dd HH:mm")).sort("timestamp"))

# COMMAND ----------

# MAGIC %md ##Look at the Bronze Table for the Green Dataset
# MAGIC - This dataset is significantly smaller than the yellow dataset

# COMMAND ----------

greenDeltaTable = DeltaTable.forPath(spark, greenDeltaDirectory)
fullGreenHistoryDF = greenDeltaTable.history()    # get the full history of the table

statsGreenHistoryDF = fullGreenHistoryDF.select("version", "timestamp", "operation", col("operationMetrics.numOutputRows").cast(IntegerType()))

displayHTML('<b>Bronze Green Table Status</b><br><svg width="100" height="100">\
   <circle cx="50" cy="50" r="40" stroke="green" stroke-width="4" fill='+getStatus(statsGreenHistoryDF)+' />\
   Sorry, your browser does not support inline SVG.\
</svg>')

# COMMAND ----------

display(fullGreenHistoryDF)

# COMMAND ----------

# MAGIC %md #####A view of the Delta Lake Optimze function running periodically

# COMMAND ----------

display(statsGreenHistoryDF.filter(statsGreenHistoryDF.operation.like('%OPTIMIZE%')).withColumn("prettyTimestamp", date_format(col("timestamp"), "MMM-dd HH:mm")).sort("timestamp"))

# COMMAND ----------

# MAGIC %md #####Show the number of rows being added to the table over time

# COMMAND ----------

display(statsGreenHistoryDF.filter(statsGreenHistoryDF.operation.like('%STREAMING%')).withColumn("prettyTimestamp", date_format(col("timestamp"), "MMM-dd HH:mm")).sort("timestamp"))

# COMMAND ----------

# MAGIC %md ##Look at the Bronze Table for the For Hire Vehicle Dataset
# MAGIC - This dataset did not make it to the silver table because it does not contain key information required for inclusion

# COMMAND ----------

fhvDeltaTable = DeltaTable.forPath(spark, fhvDeltaDirectory)
fullFhvHistoryDF = fhvDeltaTable.history()    # get the full history of the table
statsFhvHistoryDF = fullFhvHistoryDF.select("version", "timestamp", "operation", col("operationMetrics.numOutputRows").cast(IntegerType()))

displayHTML('<b>Bronze For Hire Vehicle Table Status</b><br><svg width="100" height="100">\
   <circle cx="50" cy="50" r="40" stroke="green" stroke-width="4" fill='+getStatus(statsFhvHistoryDF)+' />\
   Sorry, your browser does not support inline SVG.\
</svg>')

# COMMAND ----------

display(fullFhvHistoryDF)

# COMMAND ----------

display(statsFhvHistoryDF)

# COMMAND ----------

# MAGIC %md #####A view of the Delta Lake Optimze function running periodically

# COMMAND ----------

display(statsFhvHistoryDF.filter(statsFhvHistoryDF.operation.like('%OPTIMIZE%')).withColumn("prettyTimestamp", date_format(col("timestamp"), "MMM-dd HH:mm")).sort("timestamp"))

# COMMAND ----------

# MAGIC %md #####Show the number of rows being added to the table over time

# COMMAND ----------

display(statsFhvHistoryDF.filter(statsFhvHistoryDF.operation.like('%STREAMING%')).withColumn("prettyTimestamp", date_format(col("timestamp"), "MMM-dd HH:mm")).sort("timestamp"))

# COMMAND ----------

# MAGIC %md ##Look at the Silver Table

# COMMAND ----------

silverDeltaTable = DeltaTable.forPath(spark, silverDeltaDirectory)
fullSilverHistoryDF = silverDeltaTable.history()    # get the full history of the table
statsSilverHistoryDF = fullSilverHistoryDF.select("version", "timestamp", "operation", col("operationMetrics.numOutputRows").cast(IntegerType()))

displayHTML('<b>Silver Table Status</b><br><svg width="100" height="100">\
   <circle cx="50" cy="50" r="40" stroke="green" stroke-width="4" fill='+getStatus(statsSilverHistoryDF)+' />\
   Sorry, your browser does not support inline SVG.\
</svg>')

# COMMAND ----------

display(fullSilverHistoryDF)

# COMMAND ----------

display(statsSilverHistoryDF)

# COMMAND ----------

# MAGIC %md #####A view of the Delta Lake Optimze function running periodically

# COMMAND ----------

display(statsSilverHistoryDF.filter(statsSilverHistoryDF.operation.like('%OPTIMIZE%')).withColumn("prettyTimestamp", date_format(col("timestamp"), "MMM-dd HH:mm")).sort("timestamp"))

# COMMAND ----------

# MAGIC %md #####Show the number of rows being added to the table over time

# COMMAND ----------

display(statsSilverHistoryDF.filter(statsSilverHistoryDF.operation.like('%STREAMING%')).withColumn("prettyTimestamp", date_format(col("timestamp"), "MMM-dd HH:mm")).sort("timestamp"))

# COMMAND ----------

# MAGIC %md ##Look at the Gold Table
# MAGIC - No optimization needed on the table yet
# MAGIC - Number of records incoming are much lower

# COMMAND ----------

goldDeltaTable = DeltaTable.forPath(spark, goldDeltaDirectory)
fullGoldHistoryDF = goldDeltaTable.history()    # get the full history of the table
statsGoldHistoryDF = fullGoldHistoryDF.select("version", "timestamp", "operation", col("operationMetrics.numOutputRows").cast(IntegerType()))

displayHTML('<b>Gold Table Status</b><br><svg width="100" height="100">\
   <circle cx="50" cy="50" r="40" stroke="green" stroke-width="4" fill='+getStatus(statsGoldHistoryDF)+' />\
   Sorry, your browser does not support inline SVG.\
</svg>')

# COMMAND ----------

display(fullGoldHistoryDF)

# COMMAND ----------

display(statsGoldHistoryDF)

# COMMAND ----------

# MAGIC %md #####A view of the Delta Lake Optimze function running periodically

# COMMAND ----------

display(statsGoldHistoryDF.filter(statsGoldHistoryDF.operation.like('%OPTIMIZE%')).withColumn("prettyTimestamp", date_format(col("timestamp"), "MMM-dd HH:mm")).sort("timestamp"))

# COMMAND ----------

# MAGIC %md #####Show the number of rows being added to the table over time

# COMMAND ----------

display(statsGoldHistoryDF.filter(statsGoldHistoryDF.operation.like('%STREAMING%')).withColumn("prettyTimestamp", date_format(col("timestamp"), "MMM-dd HH:mm")).sort("timestamp"))

# COMMAND ----------

# MAGIC %md #Stop the streams
# MAGIC - UNCOMMENT WHEN WANT TO STOP STREAMS - Iterates over all active streams - Can also press Stop Execution at the top.

# COMMAND ----------

# UNCOMMENT WHEN WANT TO STOP STREAMS - Iterates over all active streams - Can also press Stop Execution at the top.
# for s in spark.streams.active:  # Iterate over all active streams
#    print("Stopping "+s.name)   # A little extra feedback
#    s.stop()                    # Stop the stream       