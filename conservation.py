# Databricks notebook source
from datetime import datetime
from pyspark.sql.functions import dayofmonth

#Connection configuration
spark.conf.set(
"fs.azure.account.key.hnadirstg.dfs.core.windows.net", "iyMubLxTlSB0r/ZL+bQr2H9LLDVc20DunWUDOVQekQWG8W1xeb5pAwTpaeozRni2AM0Ak8po1V/s+AStnE1kcQ=="
)

raw = "abfss://public-transport-data@hnadirstg.dfs.core.windows.net/raw/"
archived = "abfss://public-transport-data@hnadirstg.dfs.core.windows.net/raw/"

raw_files = dbutils.fs.ls(raw)
archived_files = dbutils.fs.ls(archived)

#Archived data to folder archive
for r in raw_files:
    modification_time_ms = r.modificationTime
    modification_time = datetime.fromtimestamp(modification_time_ms / 1000)  # Divide by 1000 to convert milliseconds to seconds
    datenow = datetime.now()
    duration =  datenow - modification_time
    if(duration.days > 15):
        #Archive data
        dbutils.fs.cp(r.path,'abfss://public-transport-data@hnadirstg.dfs.core.windows.net/archived/'+r.name,recurse=True)
        #Delete data after archive
        dbutils.fs.rm(r.path, recurse=True)
        print('Folder : '+r.name+' archived with successfuly!')

#Deleted data from folder archive
for r in archived_files:
    modification_time_ms = r.modificationTime
    modification_time = datetime.fromtimestamp(modification_time_ms / 1000)  # Divide by 1000 to convert milliseconds to seconds
    datenow = datetime.now()
    duration =  datenow - modification_time
    if(duration.days > 30):
        #Delete data from folder archive
        dbutils.fs.rm(r.path, recurse=True)
        print('Folder : '+r.name+' deleted with successfuly!')



# COMMAND ----------


