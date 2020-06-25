# Databricks notebook source
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