from  pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark=SparkSession.builder.master("local[1]").appName("Krishna").getOrCreate()

data=[("My name is Krishna",),
      ("I am learning PySpark",),
      ("I am from Delhi",)]

sample_dataframe=spark.createDataFrame(data,["Intro"])

sample_dataframe.show(truncate=False)


# rpad() - Right-pad text to a specified length with a specified string
sample_dataframe.select(rpad(col("Intro"), 25, "#").alias("rpad_text")).show(truncate=False)

# lpad() - Left-pad text to a specified length with a specified string
sample_dataframe.select(lpad(col("Intro"), 25, "#").alias("lpad_text")).show(truncate=False)

