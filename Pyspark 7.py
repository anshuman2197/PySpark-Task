# Databricks notebook source
# Write a solution to find all dates' Id with higher
# temperatures compared to its previous dates (yesterday).
# Return the result table in any order.

# COMMAND ----------

from pyspark.sql import SparkSession,Window
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark=SparkSession.builder.appName("Task 7").getOrCreate()

# COMMAND ----------

weather_schema = StructType([
StructField("id", IntegerType(), True),
StructField("recordDate", StringType(), True),
StructField("temperature", IntegerType(), True)
])

weather_data = [
(1, '2015-01-01', 10),
(2, '2015-01-02', 25),
(3, '2015-01-03', 20),
(4, '2015-01-04', 30)
]

# COMMAND ----------

df=spark.createDataFrame(weather_data,schema=weather_schema)
df.show()

# COMMAND ----------

lag=df.withColumn("prev_day",lag(df.temperature).over(Window.orderBy(df.recordDate)))
lag.show()

# COMMAND ----------

lag.filter(lag["temperature"]>lag["prev_day"]).select('id').show()

# COMMAND ----------


