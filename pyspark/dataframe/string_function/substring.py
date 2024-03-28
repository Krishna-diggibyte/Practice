from  pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark=SparkSession.builder.master("local[1]").appName("Krishna").getOrCreate()

data=[("My name is Krishna",),
      ("I am learning PySpark",),
      ("I am from Delhi",)]

sample_dataframe=spark.createDataFrame(data,["Intro"])

sample_dataframe.show(truncate=False)


# substring_index() - Extract a substring from text before a specified delimiter
sample_dataframe.select(substring_index(col("Intro"), " ", 1).alias("first_word")).show(truncate=False)

# substring() - Extract a substring from text
sample_dataframe.select(substring(col("Intro"), 2, 5).alias("substring_text")).show(truncate=False)
