# Databricks notebook source
# MAGIC %md
# MAGIC #

# COMMAND ----------

#Processus ETL avec Azure Databricks
from pyspark.sql.functions import year, month, dayofmonth, dayofweek, to_date, unix_timestamp, from_unixtime, split,when,hour
from pyspark.sql.types import IntegerType, StringType
import datetime


#Connection configuration
spark.conf.set(
"fs.azure.account.key.hnadirstg.blob.core.windows.net", "iyMubLxTlSB0r/ZL+bQr2H9LLDVc20DunWUDOVQekQWG8W1xeb5pAwTpaeozRni2AM0Ak8po1V/s+AStnE1kcQ=="
)


#Function to tronsformation file by name
def transformationData(filePath):
    #======> Transformations de Date <=======#
    spark_df = spark.read.format('csv').option('header', True).load("wasbs://public-transport-data@hnadirstg.blob.core.windows.net/raw/"+filePath+"/*.csv")

    #
    #Convert columns date to type date
    #
    spark_df = spark_df.withColumn('Date',to_date(spark_df['Date']))

    #Add columns year, month, day, and day of the week
    spark_df = spark_df.withColumn("Year", year(spark_df["Date"]).cast(IntegerType()))
    spark_df = spark_df.withColumn("Month", month(spark_df["Date"]).cast(IntegerType()))
    spark_df = spark_df.withColumn("DayOfMonth", dayofmonth(spark_df["Date"]).cast(IntegerType()))
    spark_df = spark_df.withColumn("DayOfWeek", dayofmonth(spark_df["Date"]).cast(IntegerType()))

    #Cast type of column Delay to integer
    spark_df.withColumn("Delay",spark_df.Delay.cast(IntegerType()))

    #Cast type of columnPassengers to integer
    spark_df.withColumn("Passengers",spark_df.Passengers.cast(IntegerType()))

    #======> Calculs Temporels <=======#
    def getMinuts(c):
        hours = split(c, ':')[0]
        minutes = split(c, ':')[1]
        return hours * 60 + minutes

    spark_df = spark_df.withColumn("Duration",getMinuts(spark_df['ArrivalTime']) - getMinuts(spark_df['DepartureTime']))

    #======> Analyse des Retards <=======#

    spark_df = spark_df.withColumn("Retard",when(spark_df["Delay"] <= 0, 'Pas de Retard').when(spark_df["Delay"] <= 10, "Retard Court").when(spark_df["Delay"] <= 20, "Retard Moyen").otherwise( 'Long Retard'))

    spark_df.coalesce(1).write.format("com.databricks.spark.csv").option("header","true").save("wasbs://public-transport-data@hnadirstg.blob.core.windows.net/processed/transformed/"+filePath)

    #Analyse des Passagers
    analyseDesPassagers(spark_df,filePath)

    #Analyse des Itinéraires
    analyseDesItineraires(spark_df,filePath)

    return spark_df

#======> Analyse des Passagers <=======#
def analyseDesPassagers(saprkDF,filePath):
    df = saprkDF.groupby(hour('DepartureTime')).agg({"Passengers": "avg"})
    df = df.withColumn('Pointe',when(df["avg(Passengers)"] >= 50, ' heures de pointe').when(df["avg(Passengers)"] < 50 ,'hors pointe'))
    #Renamed columns
    df = df.withColumnRenamed('hour(DepartureTime)','Hour')
    df =df.withColumnRenamed('avg(Passengers)','AVG Passengers')

    #Saved files in Analyse Passagers
    df.coalesce(1).write.format("com.databricks.spark.csv").option("header","true").save("wasbs://public-transport-data@hnadirstg.blob.core.windows.net/processed/analyse_passagers/"+filePath)

    return df

#======> Analyse des Itinéraires <=======#

def analyseDesItineraires(saprkDF,filePath):
    df = saprkDF.groupby('Route').agg({'Delay':'avg','Passengers': 'avg','Route':'count'})
    #Renamed columns with new names
    df = df.withColumnRenamed('avg(Passengers)','AVG Passengers')
    df =df.withColumnRenamed('avg(Delay)','AVG Delay')

    #Saved files in Analyse des Itinéraires
    df.coalesce(1).write.format("com.databricks.spark.csv").option("header","true").save("wasbs://public-transport-data@hnadirstg.blob.core.windows.net/processed/analyse_itineraires/"+filePath)
    return df


# COMMAND ----------

#Connection configuration
spark.conf.set(
"fs.azure.account.key.hnadirstg.dfs.core.windows.net", "iyMubLxTlSB0r/ZL+bQr2H9LLDVc20DunWUDOVQekQWG8W1xeb5pAwTpaeozRni2AM0Ak8po1V/s+AStnE1kcQ=="
)

raw = "abfss://public-transport-data@hnadirstg.dfs.core.windows.net/raw/"
processed = "abfss://public-transport-data@hnadirstg.dfs.core.windows.net/processed/transformed"

processed_files = dbutils.fs.ls(processed)
raw_files = dbutils.fs.ls(raw)

processed_files_csv = [f.name for f in processed_files]

prcessed_items_count = 0;

for f_raw in raw_files:
    if prcessed_items_count == 2:
        break
    if f_raw.name not in processed_files_csv:
        prcessed_items_count+=1
        #Transformation and save file csv
        transformationData(f_raw.name)
