from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("Krishna").getOrCreate()

data = spark.sparkContext.parallelize([2,3,4,5])

new_rdd1=data.flatMap(lambda x:range(1,x))

print(new_rdd1.collect())