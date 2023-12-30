# Databricks notebook source
# Write a pyspark code that reports the device that is first
# logged in for each player.
# Return the result table in any order.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


# COMMAND ----------

spark = SparkSession.builder.appName("FirstLogin").getOrCreate()

# Define the schema for the "Activity"
activity_schema = StructType([
    StructField("player_id", IntegerType(), True),
    StructField("device_id", IntegerType(), True),
    StructField("event_date", StringType(), True),
    StructField("games_played", IntegerType(), True)
])

# Define data for the "Activity"
activity_data = [
    (1, 2, '2016-03-01', 5),
    (1, 2, '2016-05-02', 6),
    (2, 3, '2017-06-25', 1),
    (3, 1, '2016-03-02', 0),
    # Add more data if needed
]


# COMMAND ----------

# Create a PySpark DataFrame
activity_df = spark.createDataFrame(activity_data, schema=activity_schema)


# COMMAND ----------

# Create a window specification
window_spec = Window.partitionBy("player_id").orderBy(col("event_date"))
# window_spec.show()

# COMMAND ----------

# Use row_number to get the first login for each player
result_df = activity_df.withColumn("row_num", row_number().over(window_spec))


# COMMAND ----------

# Filter the rows with rank 1 to get the first login for each player
result_df = result_df.filter(col("row_num") == 1).drop("row_num")

# Show the result
result_df.show()

# COMMAND ----------


