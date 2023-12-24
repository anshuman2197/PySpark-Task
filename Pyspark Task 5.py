# Databricks notebook source
# Write a Pyspark program to report all the duplicate emails.
# Note that it's guaranteed that the email field is not NULL.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types  import *
from pyspark.sql.functions import *

# COMMAND ----------

emails_schema = StructType([
StructField("id", IntegerType(), True),
StructField("email", StringType(), True)
])

emails_data = [
(1, 'a@b.com'),
(2, 'c@d.com'),
(3, 'a@b.com')
]

df=spark.createDataFrame(emails_data,schema=emails_schema)
df.show()

# COMMAND ----------

df_group=df.groupBy("email").count()
df_group.show()

# COMMAND ----------

df_group.filter(df_group["count"]>1).show()

# COMMAND ----------


