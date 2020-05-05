# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Save the initial timestamp from which to get records

# COMMAND ----------

from pyspark.sql import Row
from datetime import datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta

dateFormat = "%Y%m%d_%H%M"

#adjust for timezone, and month and year to start from
lastTimestamp=datetime.now()- timedelta(hours=4)- relativedelta(months=15)- relativedelta(days=20)
#adjust the time in the next notebook for the initial number of days to load

ts = lastTimestamp.strftime(dateFormat)
newDf = sc.parallelize([Row(lastTimestamp=ts)]).toDF()
newDf.coalesce(1).write.format("com.databricks.spark.csv").mode('overwrite').option("header", "true").save("/mnt/xferblob/ingest/lastTimestamp.csv")
display(newDf)