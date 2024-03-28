from  pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark=SparkSession.builder.master("local[1]").appName("Krishna").getOrCreate()

data=[("My name is Krishna",),
      ("I am learning PySpark",),
      ("I am from Delhi",)]

sample_dataframe=spark.createDataFrame(data,["Intro"])

sample_dataframe.show(truncate=False)



# translate() - replace characters in the text
sample_dataframe.select(translate(col("Intro"), "aeiou", "12345").alias("translated_text")).show(truncate=False)
