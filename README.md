# NYC Taxis

PLEASE NOTE THAT THIS IS STILL INCOMPLETE

## Setup

### Download the Databricks CLI

All commands should be executed from the project home directory

### Download NYC Taxi Metadata

[Taxi Zone Lookup Data](https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv)

[Taxi Zone Shapefiles](https://s3.amazonaws.com/nyc-tlc/misc/taxi_zones.zip)

Copy taxi data to the following DBFS location

 * dbfs:/shared/nyctaxi/nyc_taxi_zones.wkt.csv
 * dbfs:/shared/nyctaxi/taxi_zone_lookup.csv

### Create Clusters

    databricks clusters create --json-file clusters/azure/simulator.json > simulator-id.json

### Import Notebooks

    databricks workspace import_dir ./notebooks/ /Shared/NewYorkTaxi/

### Import Jobs

    databricks jobs create --json-file jobs/taxi_simulator.json
    databricks jobs create --json-file jobs/predictTaxiTipAmount.json

### Mount Cloud Storage to Databricks File System

 * [Amazon S3](https://docs.databricks.com/data/data-sources/aws/amazon-s3.html)
 * [Azure Blob Storage](https://docs.databricks.com/data/data-sources/azure/azure-storage.html)

Create the following 5 mount points

 * /mnt/xferblob/ingest
 * /mnt/xferblob/bronze
 * /mnt/xferblob/silver
 * /mnt/xferblob/gold
 * /mnt/xferblob/metadata

### Run all notebooks from Setup folder

### Execute Data Pipeline

Attach the data pipeline cluster to the following notebooks and execute

 * DataPipeline/LandingZone_To_Bronze
 * DataPipeline/Bronze_To_Silver
 * DataPipeline/Silver_To_Gold

## Reference

### Export Notebooks

Need to export the notebooks to store in GitHub

    databricks workspace export_dir -o /Shared/NewYorkTaxi ./notebooks



