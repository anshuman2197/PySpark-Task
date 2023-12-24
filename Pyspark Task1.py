# Databricks notebook source
# Actors and Directors Who Cooperated At Least
# Three Times

# Write a pyspark Program for a report that provides the pairs
# (actor_id, director_id) where the actor has cooperated with
# the director at least 3 times.

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql import SparkSession

schema = StructType([
StructField("ActorId",IntegerType(),True),
StructField("DirectorId",IntegerType(),True),
StructField("timestamp",IntegerType(),True)
])


# COMMAND ----------

spark = SparkSession.builder.appName("ActorDirectorCollaboration").getOrCreate()
data = [
(1, 1, 0),
(1, 1, 1),
(1, 1, 2),
(1, 2, 3),
(1, 2, 4),
(2, 1, 5),
(2, 1, 6)
]

# COMMAND ----------

df=spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

df_group=df.groupBy('ActorId','DirectorId').count()
df_group.show()

# COMMAND ----------

df_group.filter(df_group['count']>=3).show()

# COMMAND ----------


