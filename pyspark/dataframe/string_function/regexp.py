from  pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark=SparkSession.builder.master("local[1]").appName("Krishna").getOrCreate()

data=[("My name is Krishna1",),
      ("I am learning PySpark2",),
      ("I am from Delhi0",)]

sample_dataframe=spark.createDataFrame(data,["Intro"])

sample_dataframe.show(truncate=False)

# regexp_replace() - Replace text matching a regex pattern with a replacement string
sample_dataframe.select(regexp_replace(col("Intro"), "\\s+", "+").alias("+_inbetween_text")).show(truncate=False)
sample_dataframe.withColumn("Intro",regexp_replace("Intro", "am", "4M")).show()


# regexp_extract() - Extract substrings matching a regex pattern
sample_dataframe.select(regexp_extract(col("Intro"), "\\d+", 0).alias("digits")).show(truncate=False)
sample_dataframe.withColumn("Intro",regexp_extract("Intro", '\\d+', 0)).show()

