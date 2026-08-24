# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

df = spark.table("bikesales.bronze.products")

# COMMAND ----------

display(df)

# COMMAND ----------

df = df.withColumn(
    "CREATEDAT",
    to_date(col("CREATEDAT").cast("string"), "yyyyMMdd")
)

df = df.withColumn(
    "CHANGEDAT",
    to_date(col("CHANGEDAT").cast("string"), "yyyyMMdd")
)

# COMMAND ----------

display(df)

# COMMAND ----------

df = df.drop("WIDTH", "DEPTH", "HEIGHT", "DIMENSIONUNIT", "PRODUCTPICURL")

# COMMAND ----------

df.write.mode("overwrite") \
    .format("delta") \
    .option("mergeSchema", "true") \
    .saveAsTable("bikesales.silver.products")