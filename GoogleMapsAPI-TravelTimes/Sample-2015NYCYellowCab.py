# Databricks notebook source
# Spark 2.0, Auto-updating Scala 2.10, 30 GB
# Cluster2: i2.2xlarge; 8 Workers, 488 GB Memory, 64 Cores, 24 DBU; 1 Driver, 61 GB Memory, 8 Cores, 3 DBU 
from pyspark.sql.functions import * #lit, col, udf, unix_timestamp
from pyspark.sql import DataFrame
from pyspark.sql.types import *

# Vanilla Python libraries
import json
import urllib2
import base64
import time
from urllib2 import urlopen, Request
import re
import datetime
from datetime import datetime, timedelta

TotalSampleSize = (2500/4)*30 # run NYC and Chicago taxi trips for 30 days at 625 queries per mode (4 modes) per day

NY_Proportion = float(8550405) / (8550405 + 2720546)
YellowCab_NYC_Proportion = float(146087462) / (146087462 + 19233765) # 146,087,462 yellow taxi trips for January - June 2015
GreenCab_NYC_Proportion = 1 - YellowCab_NYC_Proportion  #  19,233,765 green taxi trips for 2015

Chicago_Proportion = 2720546 / (8550405 + 2720546)

# COMMAND ----------

# MAGIC %md ## Read in CSV file

# COMMAND ----------

import urllib

urllib.urlretrieve("https://s3-us-west-2.amazonaws.com/nyctlc/yellow_tripdata_2015-01-06.csv.gz")

# COMMAND ----------

dbutils.fs.mv("file:/tmp/tmpy1fBlQ.gz", "dbfs:/tmp/sample_zip/yellow_tripdata_zip.csv.gz")

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/tmp/sample_zip"))

# COMMAND ----------

Yellow2015DF = spark.read.load("dbfs:/tmp/sample_zip/yellow_tripdata_zip.csv.gz", format="csv", header="true")

# COMMAND ----------

# MAGIC %md ## Perform file operations

# COMMAND ----------

Yellow2015DF.count() # 77080575

# COMMAND ----------

Yellow2015DF.printSchema()

# COMMAND ----------

Yellow2015DF = Yellow2015DF.select(Yellow2015DF.tpep_pickup_datetime.alias("pickup_datetime"),
                                   Yellow2015DF.tpep_dropoff_datetime.alias("dropoff_datetime"),
                                   Yellow2015DF.pickup_longitude.cast(FloatType()).alias("pickup_longitude"),
                                   Yellow2015DF.pickup_latitude.cast(FloatType()).alias("pickup_latitude"),
                                   Yellow2015DF.dropoff_longitude.cast(FloatType()).alias("dropoff_longitude"),
                                   Yellow2015DF.dropoff_latitude.cast(FloatType()).alias("dropoff_latitude"),
                                   Yellow2015DF.passenger_count.cast(IntegerType()).alias("passenger_count"), # ByteType()
                                   Yellow2015DF.trip_distance.cast(FloatType()).alias("trip_distance"),
                                   Yellow2015DF.fare_amount.cast(FloatType()).alias("fare_amount"),
                                   Yellow2015DF.tip_amount.cast(FloatType()).alias("tip_amount"),
                                   Yellow2015DF.total_amount.cast(FloatType()).alias("total_amount"))
Yellow2015DF.printSchema()

# COMMAND ----------

Yellow2015DF.describe("passenger_count").show()

# COMMAND ----------

Yellow2015DF.describe("trip_distance").show()

# COMMAND ----------



# COMMAND ----------

SampleYellow2015DF = Yellow2015DF.sample(withReplacement = False, 
                                         fraction = float(TotalSampleSize*NY_Proportion*YellowCab_NYC_Proportion)/Yellow2015DF.count(), 
                                         seed = 100)

# COMMAND ----------

SampleYellow2015DF.count() # 12535

# COMMAND ----------

SampleYellow2015DF.show(3)

# COMMAND ----------

# MAGIC %md ## Extract Sampled Data

# COMMAND ----------

# EXPORTED VIA THIS APPROACH :) 
display(SampleYellow2015DF)

# COMMAND ----------

######### NOTE: THE BELOW MYSQL DATABASE DOES NOT EXIST ANYMORE (this is why I'm including user and password info). 
#########       THIS CODE WORKS WHEN RUN OUTSIDE OF DATABRICKS (E.G. ON MY DESKTOP WHEN USING PYSPARK IN JUPYTER NOTEBOOK). 
#SampleYellow2015DF.write \
#    .format("jdbc") \
#    .option("url", "jdbc:mysql://mytaxidbinstance.cllovddbsr70.us-west-1.rds.amazonaws.com/taxi") \
#    .option("driver", "com.mysql.jdbc.Driver") \
#    .option("dbtable", "SampleYellow2015DF") \
#    .option("user", "dmeroux") \
#    .option("password", "mhM-UMw-WA5-vvJ") \
#    .save()

# COMMAND ----------


