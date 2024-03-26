from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType ,IntegerType
spark = SparkSession.builder.master("local[1]").appName("Krishna").getOrCreate()

data=[(1,),(2,),(3,),(4,),(5,)]
column = StructType([StructField("Age",IntegerType())])
df_1= spark.createDataFrame(data,column)
df_1.show()

data2=[("Krishna",),("Basheer",),("Pratibha",)]
column2=StructType([StructField("value",StringType())])
df_2=spark.createDataFrame(data2,column2)
df_2.show()

number=[1,2,3,4,5,6,7]
df_3=spark.createDataFrame(number ,IntegerType())

df_3.show()

