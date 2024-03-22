from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("Krishna").getOrCreate()

# Create an RDD
rdd=spark.sparkContext.parallelize([1,2,3,4,6,7])

# Apply filter operation to keep only even numbers
filterd_rdd=rdd.filter(lambda  x: x%2==0)

# Print the result
print(filterd_rdd.collect())


