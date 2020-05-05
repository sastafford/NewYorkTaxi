# Databricks notebook source
from datetime import datetime, timedelta

from pyspark.sql.functions import col

from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

import mlflow
import mlflow.spark

# COMMAND ----------

# DBTITLE 1,Set experiment
experiment_name = "/Shared/NewYorkTaxi/Models/nyc_taxi_tip_predictor"
experiment = mlflow.get_experiment_by_name(experiment_name)
if experiment is None:
  experiment_id = mlflow.create_experiment(experiment_name)
mlflow.set_experiment(experiment_name)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC NYC Taxi Data was seeded with data between the dates of 16-Jan-2019 and 16-Feb-2019.  Data is being streamed in real time starting at 16-Feb-2019.  The model training needs 30 days of data and the from/to dates are adjusted based on these dates.

# COMMAND ----------

# DBTITLE 1,Filter data to previous 30 days
delta_weeks = 61
delta_days = 0
date_to = datetime.today() - timedelta(weeks=delta_weeks, days=delta_days)

days_to_subtract = 30
date_from = date_to - timedelta(days=days_to_subtract)

print(str(date_from) + " to " + str(date_to))

# COMMAND ----------

# DBTITLE 1,Get silver taxi delta table
taxi_silver_directory = "/mnt/xferblob/silver/delta/nyc-taxi-silver"
df = spark.read.format("delta").load(taxi_silver_directory)
taxiDf = df.where((df.pickupDateTime > date_from) & (df.pickupDateTime < date_to))
taxiDf.cache()

# COMMAND ----------

# DBTITLE 1,Assemble the Machine Learning Pipeline
vectorAssembler = VectorAssembler(inputCols = ['pickupLocationId', 'tripDistance', 'passengerCount'], outputCol = 'features', handleInvalid="skip")
linearRegressionEstimator = LinearRegression(featuresCol = 'features', labelCol='tipAmount')
stages = [vectorAssembler, linearRegressionEstimator]
pipeline = Pipeline().setStages(stages)

# COMMAND ----------

# DBTITLE 1,Used to evaluate against the test data set
evaluator = RegressionEvaluator(predictionCol="prediction", labelCol="tipAmount", metricName="rmse")

# COMMAND ----------

# DBTITLE 1,How many taxi rides the past 30 days?
taxiZoneCount = taxiDf.count()

# COMMAND ----------

# DBTITLE 1,Split into training and test data sets
(trainingDf, testDf) = taxiDf.randomSplit([0.7, 0.3])

# COMMAND ----------

# DBTITLE 1,Train/Evaluate the taxi tip model, log to mlFlow registry
with mlflow.start_run():
  mlflow.set_tag('author', 'sstafford')
  mlflow.set_tag('taxiRunCount', taxiZoneCount)
  mlflow.set_tag('date_from', date_from)
  mlflow.set_tag('date_to', date_to)
  model = pipeline.fit(trainingDf)
  test_metric = evaluator.evaluate(model.transform(testDf))
  mlflow.log_metric('test_' + evaluator.getMetricName(), test_metric) # Logs additional metrics
  mlflow.spark.log_model(spark_model=model, artifact_path="nycTaxiTipPrediction", 
                           registered_model_name="nyc_taxi_tip_predictor")
