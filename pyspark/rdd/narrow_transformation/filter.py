from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("Krishna").getOrCreate()

# Create an RDD
rdd=spark.sparkContext.parallelize([5,4,3,4,6,3,2])

# Apply filter operation to keep only even numbers
filte_rdd=rdd.filter(lambda  x: x%2==0)

# Print the result
print(filte_rdd.collect())


