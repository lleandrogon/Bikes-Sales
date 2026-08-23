# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
import os

# COMMAND ----------

raw_path = "/Volumes/bikesales/raw/files/"

files = [
    "Addresses.csv",
    "BusinessPartners.csv",
    "Employees.csv",
    "ProductCategories.csv",
    "ProductCategoryText.csv",
    "ProductTexts.csv",
    "Products.csv",
    "SalesOrderItems.csv",
    "SalesOrders.csv"
]

# COMMAND ----------

for file in files:
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("encoding", "ISO-8859-1") \
        .csv(raw_path + file)

    table_name = os.path.splitext(file)[0]

    df.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"bikesales.bronze.{table_name}")