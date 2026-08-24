# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
from pyspark.sql.functions import *

# COMMAND ----------

df = spark.table("bikesales.bronze.producttexts")

# COMMAND ----------

display(df)

# COMMAND ----------

# DBTITLE 1,Análise LONG_DESCR
df.groupBy("PRODUCTID") \
    .count() \
    .filter(col("count") > 1) \
    .display()

# COMMAND ----------

# DBTITLE 1,Descobrindo chave primária
df.write.mode("overwrite") \
    .format("delta") \
    .option("mergeSchema", "true") \
    .saveAsTable("bikesales.silver.producttexts")