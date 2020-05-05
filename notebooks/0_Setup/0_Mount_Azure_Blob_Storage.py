# Databricks notebook source
# MAGIC %md #This notebook contains several separate blocks for initial processing when setting up the simulator and datapipeline system
# MAGIC - Do NOT do a run all on this notebook
# MAGIC - To setup the pipeline: 1) Mount the Azure Blob Storage 2) Create the initial timestamp from when to start the processing (match the starting timestamp in the simulator to this one), and 3) Create the metadata tables (two methods here so read before running)
# MAGIC - Quick codeblocks are included to cleanout and remove all the data from the directories to start processing from scratch

# COMMAND ----------

# MAGIC %md ###Setup the mount to the Azure Blob Storage

# COMMAND ----------

#mount the blob storage - one time process
storage_account_name = "simulatortransferblob"
dbutils.fs.mount( source = "wasbs://ingest@simulatortransferblob.blob.core.windows.net/", 
                 mount_point = "/mnt/xferblob/ingest", 
                 extra_configs = {"fs.azure.account.key.simulatortransferblob.blob.core.windows.net":dbutils.secrets.get(scope = "simulatorscope", key = "transferblob")})

# COMMAND ----------

#mount the blob storage - one time process
storage_account_name = "simulatortransferblob"
dbutils.fs.mount( source = "wasbs://bronze@simulatortransferblob.blob.core.windows.net/", 
                 mount_point = "/mnt/xferblob/bronze", 
                 extra_configs = {"fs.azure.account.key.simulatortransferblob.blob.core.windows.net":dbutils.secrets.get(scope = "simulatorscope", key = "transferblob")})

# COMMAND ----------

#mount the blob storage - one time process
storage_account_name = "simulatortransferblob"
dbutils.fs.mount( source = "wasbs://silver@simulatortransferblob.blob.core.windows.net/", 
                 mount_point = "/mnt/xferblob/silver", 
                 extra_configs = {"fs.azure.account.key.simulatortransferblob.blob.core.windows.net":dbutils.secrets.get(scope = "simulatorscope", key = "transferblob")})

# COMMAND ----------

#mount the blob storage - one time process
storage_account_name = "simulatortransferblob"
dbutils.fs.mount( source = "wasbs://gold@simulatortransferblob.blob.core.windows.net/", 
                 mount_point = "/mnt/xferblob/gold", 
                 extra_configs = {"fs.azure.account.key.simulatortransferblob.blob.core.windows.net":dbutils.secrets.get(scope = "simulatorscope", key = "transferblob")})

# COMMAND ----------

#mount the blob storage - one time process
storage_account_name = "simulatortransferblob"
dbutils.fs.mount( source = "wasbs://metadata@simulatortransferblob.blob.core.windows.net/", 
                 mount_point = "/mnt/xferblob/metadata", 
                 extra_configs = {"fs.azure.account.key.simulatortransferblob.blob.core.windows.net":dbutils.secrets.get(scope = "simulatorscope", key = "transferblob")})