# Databricks notebook source
dbutils.help()

# COMMAND ----------

dbutils.data.help("summarize")

# COMMAND ----------

df1=spark.read.csv("/FileStore/resource/inventory_records.csv",header=True,inferSchema=True)
df1.display()


# COMMAND ----------

dbutils.data.summarize(df1)

# COMMAND ----------

dbutils.fs.help()
