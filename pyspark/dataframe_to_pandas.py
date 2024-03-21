import pyspark
import pandas
from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("Krishna").getOrCreate()

data=[("Krishna","Bhandari","M",50000),
      ("Kuldeep","Managoli" ,"M",50000),
      ("Pratibha","Nimbolkar","F",50000),
      ("Basheer","K","M",50000)]

columns=["first_name","last_name","gender","salary"]

pyspark_df1=spark.createDataFrame(data=data,schema=columns)
pyspark_df1.printSchema()
pyspark_df1.show(truncate=False)

pandas_df=pyspark_df1.toPandas()
print(pandas_df)