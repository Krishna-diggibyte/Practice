from pyspark.sql import SparkSession

spark= SparkSession.builder.master("local[1]").appName("Krishna").getOrCreate()

from pyspark.sql.types import StructType,StructField, StringType

schema = StructType([
  StructField('firstname', StringType(), True),
  StructField('lastname', StringType(), True)
  ])

dataframe1=spark.createDataFrame([],schema)
dataframe1.printSchema()