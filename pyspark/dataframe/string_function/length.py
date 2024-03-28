from  pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark=SparkSession.builder.master("local[1]").appName("Krishna").getOrCreate()

data=[("My name is Krishna",),
      ("I am learning PySpark",),
      ("I am from Delhi",)]

sample_dataframe=spark.createDataFrame(data,["Intro"])

sample_dataframe.show(truncate=False)


# length() - Get the length of the text
sample_dataframe.select(length(col("Intro")).alias("text_length")).show(truncate=False)
