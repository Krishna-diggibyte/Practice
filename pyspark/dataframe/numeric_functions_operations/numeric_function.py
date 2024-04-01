from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,FloatType

from pyspark.sql.functions import sum, avg, min, max, round, abs

# Initialize SparkSession
spark = SparkSession.builder.master("local[1]").appName("Krishna").getOrCreate()

# Sample DataFrame
data = [(1.11,), (2.22,), (3.35,), (4.62,),(5.55,),(-2.32,),(-4.67,)]

schema=StructType([
    StructField("value",FloatType(),True)
])
df = spark.createDataFrame(data, schema=schema)

# Sum()
df.select(round(sum("value"),2).alias("sum")).show()


# Avg()
avg_result = df.select(round(avg("value"),2).alias("avg")).show()

# Min()
min_result = df.select(round(min("value"),2).alias("min")).show()

# Max()
max_result = df.select(round(max("value"),2).alias("max")).show()

#Round()
df.select(round('value',1).alias("RoundOff")).show()

#abs()
df.select(abs('value').alias("ABS")).show()

