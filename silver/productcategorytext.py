# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

df = spark.table("bikesales.bronze.productcategorytext")

# COMMAND ----------

display(df)

# COMMAND ----------

df = df.drop("MEDIUM_DESCR", "LONG_DESCR")

# COMMAND ----------

df.write.mode("overwrite") \
    .format("delta") \
    .option("mergeSchema", "true") \
    .saveAsTable("bikesales.silver.productcategorytext")