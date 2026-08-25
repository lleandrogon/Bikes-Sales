# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

df = spark.table("bikesales.bronze.salesorders")

# COMMAND ----------

display(df)

# COMMAND ----------

df.groupBy("SALESORDERID") \
    .count() \
    .filter(col("count") > 1) \
    .orderBy(col("count").desc()) \
    .display()

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

df.display()

# COMMAND ----------

df.select("FISCVARIANT").distinct().display()

# COMMAND ----------

# DBTITLE 1,Check FISCALYEARPERIOD format
df.select("FISCALYEARPERIOD").distinct().orderBy("FISCALYEARPERIOD").display()

# COMMAND ----------

df.select("NOTEID").distinct().display()

# COMMAND ----------

df = df.drop("NOTEID")

# COMMAND ----------

display(df)

# COMMAND ----------

df = df.withColumn(
    "GROSSAMOUNT",
    col("GROSSAMOUNT").cast("double")
)

# COMMAND ----------

df.limit(100).display()

# COMMAND ----------

df.select("LIFECYCLESTATUS", "BILLINGSTATUS", "DELIVERYSTATUS").distinct().display()

# COMMAND ----------

df.write.mode("overwrite") \
    .format("delta") \
    .option("mergeSchema", "true") \
    .saveAsTable("bikesales.silver.salesorders")