from  pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark=SparkSession.builder.master("local[1]").appName("Krishna").getOrCreate()

data=[("  My name is Krishna   ",),
      ("     I am learning PySpark  ",),
      ("  I am from Delhi   ",)]

sample_dataframe=spark.createDataFrame(data,["Intro"])

sample_dataframe.show(truncate=False)


# trim() - Trim leading and trailing whitespaces
sample_dataframe.select(trim(col("Intro")).alias("trimmed_text")).show(truncate=False)

# ltrim() - Trim leading whitespaces
sample_dataframe.select(ltrim(col("Intro")).alias("ltrimmed_text")).show(truncate=False)

# rtrim() - Trim trailing whitespaces
sample_dataframe.select(rtrim(col("Intro")).alias("rtrimmed_text")).show(truncate=True)
