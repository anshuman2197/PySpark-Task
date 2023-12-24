# Databricks notebook source
# Write a Pyspark program to find all customers who never
# order anything.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

customers_schema = StructType([
StructField("id", IntegerType(), True),
StructField("name", StringType(), True)
])

customers_data = [
(1, 'Joe'),
(2, 'Henry'),
(3, 'Sam'),
(4, 'Max')
]

orders_schema = StructType([
StructField("id", IntegerType(), True),
StructField("customerId", IntegerType(), True)
])

orders_data = [
(1, 3),
(2, 1)
]

# COMMAND ----------

customers=spark.createDataFrame(customers_data,schema=customers_schema)
customers.show()

# COMMAND ----------

orders=spark.createDataFrame(orders_data,schema=orders_schema)
orders.show()

# COMMAND ----------

customers.join(orders,customers.id==orders.customerId,"left_anti").select("name").show()

# COMMAND ----------


