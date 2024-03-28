from  pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark=SparkSession.builder.master("local[1]").appName("Krishna").getOrCreate()

data=[("My name is Krishna",),
      ("I am learning PySpark",),
      ("I am from Delhi",)]

sample_dataframe=spark.createDataFrame(data,["Intro"])

sample_dataframe.show()

# upper() - Convert text to uppercase
sample_dataframe.select(upper(col("Intro")).alias("upper_text")).show(truncate=False)

# lower() - Convert text to lowercase
sample_dataframe.select(lower(col("Intro")).alias("lower_text")).show(truncate=False)

