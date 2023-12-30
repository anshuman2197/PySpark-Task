# Databricks notebook source
# Write a solution to report the name and bonus amount of
# each employee with a bonus less than 1000.
# Return the result table in any order

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# Define the schema for the "Employee"
employee_schema = StructType([
StructField("empId", IntegerType(), True),
StructField("name", StringType(), True),
StructField("supervisor", IntegerType(), True),
StructField("salary", IntegerType(), True)
])
# Define data for the "Employee"
employee_data = [
(3, 'Brad', None, 4000),
(1, 'John', 3, 1000),
(2, 'Dan', 3, 2000),
(4, 'Thomas', 3, 4000)
]
# Define the schema for the "Bonus"
bonus_schema = StructType([
StructField("empId", IntegerType(), True),
StructField("bonus", IntegerType(), True)
])
# Define data for the "Bonus"
bonus_data = [
(2, 500),
(4, 2000)
]

# COMMAND ----------

employee=spark.createDataFrame(employee_data,employee_schema)
employee.show()

# COMMAND ----------

bonus=spark.createDataFrame(bonus_data,bonus_schema)
bonus.show()

# COMMAND ----------

result=employee.join(bonus,bonus["empId"]==employee["empId"],"left")
result.show()

# COMMAND ----------

result_new=result.filter((result.bonus <1000) | col("bonus").isNull())

# COMMAND ----------

result_new.show()

# COMMAND ----------


