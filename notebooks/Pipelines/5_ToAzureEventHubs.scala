// Databricks notebook source
// MAGIC %md 
// MAGIC 
// MAGIC # Gold to Azure Event Hubs (optional)
// MAGIC 
// MAGIC Use this notebook to stream data from NYC_Taxi gold table to Azure Event Hub

// COMMAND ----------

import org.apache.spark.eventhubs.{ ConnectionStringBuilder, EventHubsConf, EventPosition }
import org.apache.spark.sql.streaming.Trigger.ProcessingTime
import org.apache.spark.sql.functions.{ to_json, struct }


// COMMAND ----------

// DBTITLE 1,Use secrets to protect sensitive information
dbutils.secrets.get(scope = "azure", key = "SharedAccessKey")

// COMMAND ----------

// DBTITLE 1,Azure Event Hub Connection
// The connection string for the Event Hub you will WRTIE to. 
val endpoint = "sb://pear-orange.servicebus.windows.net/"
val eventhub_name = "nyc-taxi-rides"
val sharedAccessKey = dbutils.secrets.get(scope = "azure", key = "SharedAccessKey")
val connString = "Endpoint=" + endpoint + ";SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=" + sharedAccessKey + ";EntityPath=" + eventhub_name
val eventHubsConfWrite = EventHubsConf(connString)

// COMMAND ----------

// DBTITLE 1,Read Gold table and transform for publish
val goldDeltaDirectory = "/mnt/xferblob/gold/delta/nyc-taxi-gold"

val source = 
  spark.readStream
    .format("delta")
    .load(goldDeltaDirectory)
    .select(to_json(
        struct(   $"vendorID",$"pickupDateTime",$"pickupDate",$"dropoffDateTime",$"dropoffDate",$"pickupLocationId",$"dropoffLocationId",$"passengerCount",$"tripDistance",$"rateCodeId",$"storeAndFwdFlag",$"paymentType",$"fareAmount",$"extra",$"mtaTax",$"improvementSurcharge",$"tipAmount",$"tollsAmount",$"totalAmount",$"pickupYear",$"pickupMonth",$"sourceDataset",$"ehailFee",$"PickupBurrough",$"PickupZone",$"DropoffBurrough",$"DropoffZone",$"features",$"prediction",$"upper",$"lower",$"tip_alert"))
            .alias("body") cast "string")

// COMMAND ----------

// DBTITLE 1,Write stream to Azure Event Hub
val query = 
  source
    .writeStream
    .format("eventhubs")
    .outputMode("update")
    .options(eventHubsConfWrite.toMap)
    .trigger(ProcessingTime("1 minute"))
    .option("checkpointLocation", "/mnt/xferblob/eventhubs/nyc-taxi-gold/checkpoint/")
    .start()