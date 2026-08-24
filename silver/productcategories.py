# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

df = spark.table("bikesales.bronze.productcategories")

# COMMAND ----------

display(df)

# COMMAND ----------

df = df.withColumn(
    "CREATEDAT",
    to_date(col("CREATEDAT").cast("string"), "yyyyMMdd")
)

# COMMAND ----------

display(df)

# COMMAND ----------

df.write.mode("overwrite") \
    .format("delta") \
    .option("mergeSchema", "true") \
    .saveAsTable("bikesales.silver.productcategories")