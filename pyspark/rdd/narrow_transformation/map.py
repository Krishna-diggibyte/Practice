from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("Krishna").getOrCreate()

num= spark.sparkContext.parallelize([2,3,1,4,5,6,5])
print(num.collect())

print(num.map(lambda x : x*2 ).collect())