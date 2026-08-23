# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

df = spark.table("bikesales.bronze.employees")

# COMMAND ----------

display(df)

# COMMAND ----------

df = df.drop("NAME_INITIALS")

# COMMAND ----------

df = df.withColumn(
    "PHONENUMBER",
    regexp_replace(col("PHONENUMBER"), "[^0-9]", "")
).withColumn(
    "IS_VALID_PHONE",
    when(length(regexp_replace(col("PHONENUMBER"), "[^0-9]", "")) >= 10, True).otherwise(False)
)

# COMMAND ----------

display(df)

# COMMAND ----------

df = df.withColumn(
    "IS_CURRENT",
    when(col("VALIDITY_ENDDATE") == 99991231, True).otherwise(False)
)

df = df.withColumn(
    "VALIDITY_STARTDATE", 
    to_date(col("VALIDITY_STARTDATE").cast("string"), "yyyyMMdd")
).withColumn(
    "VALIDITY_ENDDATE", 
    to_date(col("VALIDITY_ENDDATE").cast("string"), "yyyyMMdd")
)

# COMMAND ----------

df.select("VALIDITY_STARTDATE", "VALIDITY_ENDDATE", "IS_CURRENT").display()

# COMMAND ----------

df = df.drop("_c13", "_c14", "_c15", "_c16", "_c17", "_c18")

# COMMAND ----------

df.write.mode("overwrite") \
    .format("delta") \
    .option("mergeSchema", "true") \
    .saveAsTable("bikesales.silver.employees")