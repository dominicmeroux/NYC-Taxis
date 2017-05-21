# Databricks notebook source
# Import libraries # spark 2.1.0-db2 scala 2.11
from pyspark.sql.window import *
from pyspark.sql.functions import * #from pyspark.sql.functions import col, lag
from pyspark.sql import DataFrame
from pyspark.sql.types import *
import datetime
from datetime import datetime

# GraphFrame libraries
# MAKE SURE CLUSTER MATCHES GRAPHFRAMES PACKAGE, E.G. graphframes-0.3.0-spark2.0-s_2.11 WITH A Spark 2.0.0, Scala 2.11 Cluster
#import graphframes
#from graphframes import GraphFrame
##from graphframes.examples import Graphs

# MLlib Libraries
#from pyspark.ml.regression import LinearRegression

# COMMAND ----------

# Uploaded CSV files (I broke the original download into 536 files)
FilePath_ChicagoTaxi = '/FileStore/tables/0zon0sgv1490658299765'

DF_ChicagoTaxi = spark.read.load(FilePath_ChicagoTaxi, format="csv", header="true")

DF_ChicagoTaxi.printSchema()

# COMMAND ----------

Refined_Chicago = DF_ChicagoTaxi.select(DF_ChicagoTaxi['Trip ID'].alias("id"),
                                        DF_ChicagoTaxi['Taxi ID'].alias("taxiID"),
                                        unix_timestamp(DF_ChicagoTaxi['Trip Start Timestamp'], "MM/dd/yyyy K:mm:ss a").cast(TimestampType()).alias("pickup_datetime"),
                                        unix_timestamp(DF_ChicagoTaxi['Trip End Timestamp'], "MM/dd/yyyy K:mm:ss a").cast(TimestampType()).alias("dropoff_datetime"),
                                        round(DF_ChicagoTaxi['Pickup Centroid Latitude'].cast(DoubleType()), 1).alias("pickup_latitude"),
                                        round(DF_ChicagoTaxi['Pickup Centroid Longitude'].cast(DoubleType()), 1).alias("pickup_longitude"),
                                        concat(round(DF_ChicagoTaxi['Pickup Centroid Latitude'].cast(DoubleType()), 1), round(DF_ChicagoTaxi['Pickup Centroid Longitude'].cast(DoubleType()), 1)).alias('src'),
                                        round(DF_ChicagoTaxi['Dropoff Centroid Latitude'].cast(DoubleType()), 1).alias("dropoff_latitude"),
                                        round(DF_ChicagoTaxi['Dropoff Centroid Longitude'].cast(DoubleType()), 1).alias("dropoff_longitude"),
                                        concat(round(DF_ChicagoTaxi['Dropoff Centroid Latitude'].cast(DoubleType()), 1), round(DF_ChicagoTaxi['Dropoff Centroid Longitude'].cast(DoubleType()), 1)).alias('dst'),
                                        lit(99).cast(IntegerType()).alias("passenger_count"), ### Would do 99999, but unnecessary for passenger count :) 
                                        DF_ChicagoTaxi['Trip Miles'].cast(FloatType()).alias("trip_distance"),
                                        regexp_replace(DF_ChicagoTaxi['Fare'], "\$", "").cast(FloatType()).alias("fare_amount"),
                                        regexp_replace(DF_ChicagoTaxi['Extras'], "\$", "").cast(FloatType()).alias("extra"),
                                        lit(99999).cast(FloatType()).alias("mta_tax"),
                                        regexp_replace(DF_ChicagoTaxi['Tips'], "\$", "").cast(FloatType()).alias("tip_amount"),
                                        regexp_replace(DF_ChicagoTaxi['Tolls'], "\$", "").cast(FloatType()).alias("tolls_amount"),
                                        lit(0).cast(FloatType()).alias("ehail_fee"),
                                        lit(0).cast(FloatType()).alias("improvement_surcharge"),
                                        regexp_replace(DF_ChicagoTaxi['Trip Total'], "\$", "").cast(FloatType()).alias("total_amount"),
                                        DF_ChicagoTaxi['Company'].cast(StringType()).alias("company")).withColumn("Dataset", lit("Chicago Taxis"))
display(Refined_Chicago) 

# COMMAND ----------

# Save dataframe to Hive
Refined_Chicago.write.mode("overwrite").saveAsTable("ChicagoTrips")

# COMMAND ----------

# Make sure taxi ID values are not unique to each entry
sqlContext.sql("select taxiID, count(*) from ChicagoTrips group by taxiID").show()

# COMMAND ----------

TaxiDailyDistances = sqlContext.sql("select taxiID, count(*) as dailyTrips, avg(((day(dropoff_datetime) - day(dropoff_datetime))*24 + (hour(dropoff_datetime) - hour(pickup_datetime)) + (minute(dropoff_datetime) - minute(pickup_datetime) / 60)) / trip_distance) as avg_speed, dayofyear(pickup_datetime) as day_of_year, year(pickup_datetime) as year, sum(trip_distance) as day_distance from ChicagoTrips where trip_distance > 0 group by taxiID, dayofyear(pickup_datetime), year(pickup_datetime)")

display(TaxiDailyDistances) # Then export this out of DataBricks

# COMMAND ----------


