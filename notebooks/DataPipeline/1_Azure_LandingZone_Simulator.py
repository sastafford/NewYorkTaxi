# Databricks notebook source
# MAGIC %md 
# MAGIC # Azure Landing Zone Simulator
# MAGIC 
# MAGIC A simulator that reads from an upstream source and places data being placed in a sink (a.k.a. the landing zone).  Set up this program as a recurring job.

# COMMAND ----------

#link to the datasets to use
from azureml.opendatasets import NycTlcYellow
from azureml.opendatasets import NycTlcGreen
from azureml.opendatasets import NycTlcFhv

#imports needed
from datetime import datetime
from dateutil import parser

from pyspark.sql import Row
from pyspark.sql.types import IntegerType, DoubleType
from pyspark.sql.functions import col, lit
from datetime import timedelta
from dateutil.relativedelta import relativedelta

# COMMAND ----------

# DBTITLE 1,Find the start and end datetime of this microbatch
dateFormat = "%Y%m%d_%H%M"

#no check currently to make sure the startTimestamp is before the endTimestamp
startTimestampDf = spark.read.format('csv').options(header='true', inferSchema='true').load('/mnt/xferblob/ingest/lastTimestamp.csv')
startTimestamp = datetime.strptime(startTimestampDf.collect()[0][0], dateFormat)

endTimestamp=datetime.now() - timedelta(hours=4)- relativedelta(months=14)

ts = endTimestamp.strftime(dateFormat)
newDf = sc.parallelize([Row(lastTimestamp=ts)]).toDF()
newDf.coalesce(1).write.format("com.databricks.spark.csv").mode('overwrite').option("header", "true").save("/mnt/xferblob/ingest/lastTimestamp.csv")
display(newDf)

# COMMAND ----------

# DBTITLE 1,Get new data since timestamp
#get the next set of NEW data from each dataset
yellow_taxi_data = NycTlcYellow(start_date=startTimestamp, end_date=endTimestamp)
green_taxi_data = NycTlcGreen(start_date=startTimestamp, end_date=endTimestamp)
fhv_taxi_data = NycTlcFhv(start_date=startTimestamp, end_date=endTimestamp)

# COMMAND ----------

yellowRawDF = yellow_taxi_data.to_spark_dataframe() 

# COMMAND ----------

greenRawDF = green_taxi_data.to_spark_dataframe()

# COMMAND ----------

fhvRawDF = fhv_taxi_data.to_spark_dataframe()

# COMMAND ----------

# DBTITLE 1,For future use - incorporate later to speed up time when ingesting data
#from pyspark.sql.functions import current_timestamp

#get the start and end timestamps for the new set of data
#batch_start_ts = startTimestamp
#batch_end_ts = batch_start_ts + timedelta(minutes=5)

# COMMAND ----------

#get the data for the 5 min range into a DF

#yellowBatchDF = yellowRawDF.select('*').filter((col("tpepDropoffDateTime")>batch_start_ts) & (col("tpepDropoffDateTime")<=batch_end_ts)).orderBy("tpepDropoffDateTime")
#greenBatchDF = greenRawDF.select('*').filter((col("lpepDropoffDatetime")>batch_start_ts) & (col("lpepDropoffDatetime")<=batch_end_ts)).orderBy("lpepDropoffDatetime")
#fhvBatchDF = fhvRawDF.select('*').filter((col("dropOffDateTime")>batch_start_ts) & (col("dropOffDateTime")<=batch_end_ts)).orderBy("dropOffDateTime")


# COMMAND ----------

yellowRawDF.write \
    .format("csv") \
    .mode("append") \
    .save('/mnt/xferblob/ingest/nyc-taxi-yellow-raw')

# COMMAND ----------

greenRawDF.write \
    .format("csv") \
    .mode("append") \
    .save('/mnt/xferblob/ingest/nyc-taxi-green-raw')

# COMMAND ----------

fhvRawDF.write \
    .format("csv") \
    .mode("append") \
    .save('/mnt/xferblob/ingest/nyc-taxi-fhv-raw')