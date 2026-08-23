# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

df = spark.table("bikesales.bronze.businesspartners")

# COMMAND ----------

display(df)

# COMMAND ----------

df.groupBy("PARTNERID") \
    .count() \
    .filter(col("count") > 1) \
    .display()

# COMMAND ----------

df.select("PARTNERROLE").distinct().display()

# COMMAND ----------

df.select("EMAILADDRESS").distinct().display()

# COMMAND ----------

# DBTITLE 1,Verificar valores nulos em FAXNUMBER
df.filter(col("FAXNUMBER").isNotNull()).display()

# COMMAND ----------

df = df.drop("FAXNUMBER")

# COMMAND ----------

display(df)

# COMMAND ----------

df.filter(col("COMPANYNAME").isNull()).display()

# COMMAND ----------

df.select("LEGALFORM").distinct().display()

# COMMAND ----------

df = df.withColumn(
    "CREATEDAT", 
    to_date(col("CREATEDAT").cast("string"), "yyyyMMdd")
).withColumn(
    "CHANGEDAT", 
    to_date(col("CHANGEDAT").cast("string"), "yyyyMMdd")
)

# COMMAND ----------

display(df)

# COMMAND ----------

df.write.mode("overwrite") \
    .format("delta") \
    .option("mergeSchema", "true") \
    .saveAsTable("bikesales.silver.businesspartners")