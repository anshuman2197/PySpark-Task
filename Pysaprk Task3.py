# Databricks notebook source
# Write a Pyspark program to report the first name, last name, city, and state of each person in the
# Person dataframe. If the address of a personId is not present in the Address dataframe,
# report null instead.

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql import SparkSession


spark=SparkSession.builder.appName("Task 3").getOrCreate()

persons_schema = StructType([
StructField("personId", IntegerType(), True),
StructField("lastName", StringType(), True),
StructField("firstName", StringType(), True)
])

addresses_schema = StructType([
StructField("addressId", IntegerType(), True),
StructField("personId", IntegerType(), True),
StructField("city", StringType(), True),
StructField("state", StringType(), True)
])


# COMMAND ----------

persons_data = [
(1, 'Wang', 'Allen'),
(2, 'Alice', 'Bob')
]

addresses_data = [
(1, 2, 'New York City', 'New York'),
(2, 3, 'Leetcode', 'California')
]
persons_df = spark.createDataFrame(persons_data, schema=persons_schema)
addresses_df = spark.createDataFrame(addresses_data, schema=addresses_schema)

result=persons_df.join(addresses_df,persons_df.personId==addresses_df.personId,"left").select('firstName','lastName','city','state')

result.show()

# COMMAND ----------


