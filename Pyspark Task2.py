# Databricks notebook source
# Write an pyspark code to find the ctr of each Ad.Round ctr to 2
# decimal points. Order the result table by ctr in descending order
# and by ad_id in ascending order in case of a tie.

# Ctr=Clicked/(Clicked+Viewed)

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import  *

spark=SparkSession.builder.appName("Task 2").getOrCreate()

schema=StructType([
StructField('AD_ID',IntegerType(),True)
,StructField('USER_ID',IntegerType(),True)
,StructField('ACTION',StringType(),True)
])
data = [
(1, 1, 'Clicked'),
(2, 2, 'Clicked'),
(3, 3, 'Viewed'),
(5, 5, 'Ignored'),
(1, 7, 'Ignored'),
(2, 7, 'Viewed'),
(3, 5, 'Clicked'),
(1, 4, 'Viewed'),
(2, 11, 'Viewed'),
(1, 2, 'Clicked')
]

# COMMAND ----------

df=spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

ad_ctr = (
    df.groupBy("AD_ID")
    .agg(
        (sum(when(col("ACTION") == "Clicked", 1).otherwise(0)) /
         (sum(when(col("ACTION") == "Clicked", 1).otherwise(0)) +
          sum(when(col("ACTION") == "Viewed", 1).otherwise(0)))).alias("ctr")
    )
)
result = ad_ctr.orderBy(col("ctr").desc(), col("AD_ID").asc())
result.show()

# COMMAND ----------


