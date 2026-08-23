# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

df = spark.table("bikesales.bronze.addresses")

# COMMAND ----------

display(df)

# COMMAND ----------

df.groupBy("ADDRESSID") \
    .count() \
    .filter(col("count") > 1) \
    .display()

# COMMAND ----------

df.filter(col("CITY").isNull()).display()

# COMMAND ----------

df.filter(col("POSTALCODE").isNull()).display()

# COMMAND ----------

df.filter(col("STREET").isNull()).display()

# COMMAND ----------

df.select("CITY", "POSTALCODE", "STREET", "BUILDING").filter(col("BUILDING").isNull()).display()

# COMMAND ----------

df.filter(col("COUNTRY").isNull()).display()

# COMMAND ----------

df.select("COUNTRY").distinct().display()

# COMMAND ----------

df.select("REGION").distinct().display()

# COMMAND ----------

df.select("ADDRESSTYPE").distinct().display()

# COMMAND ----------

display(df)

# COMMAND ----------

# DBTITLE 1,Convert validity dates to date type
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

df.select("ADDRESSID", "CITY", "VALIDITY_STARTDATE", "VALIDITY_ENDDATE", "IS_CURRENT").limit(5).display()

# COMMAND ----------

df.write.mode("overwrite") \
    .format("delta") \
    .option("mergeSchema", "true") \
    .saveAsTable("bikesales.silver.addresses")