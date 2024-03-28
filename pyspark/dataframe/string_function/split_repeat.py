from  pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark=SparkSession.builder.master("local[1]").appName("Krishna").getOrCreate()

data=[("My name is Krishna ",),
      ("I am learning PySpark ",),
      ("I am from Delhi ",)]

sample_dataframe=spark.createDataFrame(data,["Intro"])

sample_dataframe.show(truncate=False)

# split() - Split text into an array of substrings based on a delimiter
sample_dataframe.select(split(col("Intro"), " ").alias("split_text")).show(truncate=False)

# repeat() - Repeat text a specified number of times
sample_dataframe.select(repeat(col("Intro"), 2).alias("repeated_text")).show(truncate=False)
