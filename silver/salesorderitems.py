# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

df = spark.table("bikesales.bronze.salesorderitems")

# COMMAND ----------

display(df)

# COMMAND ----------

df.groupBy("SALESORDERID", "SALESORDERITEM") \
    .count() \
    .filter(col("count") > 1) \
    .display()

# COMMAND ----------

df.groupBy("PRODUCTID") \
    .count() \
    .filter(col("count") > 1) \
    .display()

# COMMAND ----------

df.select("NOTEID").distinct().display()

# COMMAND ----------

df = df.drop("NOTEID")

# COMMAND ----------

df.limit(100).display()

# COMMAND ----------

df = df.withColumn(
    "GROSSAMOUNT",
    col("GROSSAMOUNT").cast("double")
)

# COMMAND ----------

display(df)

# COMMAND ----------

# DBTITLE 1,Check distinct ITEMATPSTATUS values
df.select("OPITEMPOS").distinct().display()

# COMMAND ----------

df = df.drop("OPITEMPOS")

# COMMAND ----------

df.select("QUANTITYUNIT").distinct().display()

# COMMAND ----------

df = df.withColumn(
    "DELIVERYDATE",
    to_date(col("DELIVERYDATE").cast("string"), "yyyyMMdd")
)

# COMMAND ----------

df.limit(50).display()

# COMMAND ----------

df.write.mode("overwrite") \
    .format("delta") \
    .option("mergeSchema", "true") \
    .saveAsTable("bikesales.silver.salesorderitems")