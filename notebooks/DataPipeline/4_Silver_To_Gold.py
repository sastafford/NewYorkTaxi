# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Silver to Gold
# MAGIC 
# MAGIC This notebook results in the creation of the NYC_Taxi Gold table.  It checks out the tip prediction model from the mlFlow registry and applies the tip prediction to the latest stream of data.  It adds an upper tip amount and if the tip amount is higher than the upper predicted tip amount, a tip alert flag is set

# COMMAND ----------

from pyspark.sql.functions import when

import mlflow
import mlflow.spark
from mlflow.tracking import MlflowClient

# COMMAND ----------

# DBTITLE 1,Get latest NYC Taxi Tip Model
client = MlflowClient()

def getModelName():
  return "nyc_taxi_tip_predictor"

def getModelVersions(model_name):
  return client.search_model_versions("name='" + model_name + "'")

def getTaxiTipPredictorModel(modelVersion):
  source = modelVersion.source
  return mlflow.spark.load_model(source)

def getRootMeanSquareError(modelVersion):
  run_id = modelVersion.run_id
  run = mlflow.get_run(run_id)
  run_data = run.data
  return run_data.metrics["test_rmse"]


# COMMAND ----------

# DBTITLE 1,Get tip predictor model from mlFlow registry
modelVersions = getModelVersions(getModelName())
modelVersion = modelVersions[0]
taxiTipPredictorModel = getTaxiTipPredictorModel(modelVersion)
rmse = getRootMeanSquareError(modelVersion)
(taxiTipPredictorModel, rmse)

# COMMAND ----------

# DBTITLE 1,Read the NYC Taxi silver table
silverDirectory = "/mnt/xferblob/silver/delta/nyc-taxi-silver"
df = spark.readStream.format("delta").load(silverDirectory)

# COMMAND ----------

# DBTITLE 1,Apply the tip prediction model to the streaming silver table stream and set the tip alert flag
predictionDf = taxiTipPredictorModel.transform(df)
predictionWithLimitsDf = (predictionDf
                          .withColumn("upper", predictionDf.prediction + (2 * rmse))
                          .withColumn("lower", predictionDf.prediction - (2 * rmse)))  
flagDf = (predictionWithLimitsDf
          .withColumn("tip_alert", 
                      when(
                        (predictionWithLimitsDf.tipAmount > predictionWithLimitsDf.upper), True)
                      .otherwise(False)))


# COMMAND ----------

# DBTITLE 1,Write out the NYC Taxi gold delta table
(flagDf.writeStream 
  .format("delta") 
  .outputMode("append") 
  .option("mergeSchema", "true") 
  .option("checkpointLocation", "/mnt/xferblob/gold/checkpoint/nyc-taxi-gold/") 
  .partitionBy("pickupDate")
  .start("/mnt/xferblob/gold/delta/nyc-taxi-gold"))