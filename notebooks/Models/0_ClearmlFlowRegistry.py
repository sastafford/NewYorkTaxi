# Databricks notebook source
# DBTITLE 1,Delete all models from mlFlow Registry
import mlflow
from mlflow.tracking import MlflowClient

client = MlflowClient()
for rm in client.list_registered_models():
    print(rm.name)
    #client.delete_registered_model(name=rm.name)

# COMMAND ----------

experiments = client.list_experiments()
for experiment in experiments:
  print(experiment)

#mlflow.delete_experiment(3800714290721818)