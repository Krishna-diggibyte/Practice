from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType ,IntegerType
spark = SparkSession.builder.master("local[1]").appName("Krishna").getOrCreate()


number=[1,2,3,4,5,6,7]
df_3=spark.createDataFrame(number ,IntegerType())

print(df_3.count())

