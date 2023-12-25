# Databricks notebook source
# Write a solution to find the first login date for each player.
# Return the result table in any order.

# COMMAND ----------

from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

spark=SparkSession.builder.appName("Task 8").getOrCreate()

activity_schema = StructType([
StructField("player_id", IntegerType(), True),
StructField("device_id", IntegerType(), True),
StructField("event_date", StringType(), True),
StructField("games_played", IntegerType(), True)
])

activity_data = [
(1, 2, '2016-03-01', 5),
(1, 2, '2016-05-02', 6),
(2, 3, '2017-06-25', 1),
(3, 1, '2016-03-02', 0),
(3, 4, '2018-07-03', 5)
]

df=spark.createDataFrame(activity_data,schema=activity_schema)

# COMMAND ----------

df.show()

# COMMAND ----------

rank=df.withColumn("RK",rank().over(Window.partitionBy(df["player_id"]).orderBy(df["event_date"])))

# COMMAND ----------

rank.show()

# COMMAND ----------

rank.filter(rank["RK"]==1).select("player_id",rank["event_date"].alias("First login")).show()

# COMMAND ----------


