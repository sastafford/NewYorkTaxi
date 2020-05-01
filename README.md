# NYC Taxis

## Demo Setup

### NYC Taxi Metadata

[Taxi Zone Lookup Data](https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv)

[Taxi Zone Shapefiles](https://s3.amazonaws.com/nyc-tlc/misc/taxi_zones.zip)

### [Create a secret scope](https://docs.databricks.com/security/secrets/example-secret-workflow.html#example-secret-workflow)

#### Azure

    databricks secrets create-scope --scope simulatorscope
    databricks secrets put --scope azure --key transferblob

    databricks secrets create-scope --scope azure
    databricks secrets put --scope azure --key SharedAccessKey

## Export Notebooks

Need to export the notebooks to store in GitHub

    databricks workspace export_dir /Shared/NewYorkTaxi ./NewYorkTaxi



