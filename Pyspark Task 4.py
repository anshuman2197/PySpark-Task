# Databricks notebook source
# 04_Employees Earning More Than Their Managers
# Write a Pyspark program to find Employees Earning More Than Their
# Managers

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col


spark=SparkSession.builder.appName("Task 4").getOrCreate()

# COMMAND ----------

employees_schema = StructType([
StructField("id", IntegerType(), True),
StructField("name", StringType(), True),
StructField("salary", IntegerType(), True),
StructField("managerId", IntegerType(), True)
])

employees_data = [
(1, 'Joe', 70000, 3),
(2, 'Henry', 80000, 4),
(3, 'Sam', 60000, None),
(4, 'Max', 90000, None)
]


# COMMAND ----------

df=spark.createDataFrame(employees_data,schema=employees_schema)
df.show()

# COMMAND ----------

emp1=df.alias("e1")
emp2=df.alias("e2")

joined=emp1.join(emp2,col("e1.id")==col("e2.managerId"),"inner")
final=joined.select(col("e2.name"),col("e2.salary"),col("e1.salary").alias("msal"))

self_final=final.filter(final.salary>final.msal)
self_final.show()

# COMMAND ----------


