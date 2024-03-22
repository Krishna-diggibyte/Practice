from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.appName("Krishna").getOrCreate()



# Create an RDD
rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5])

# Print first two elements
print(rdd.take(2))
#top two ie- last two
print(rdd.top(2))

# Stop SparkSession
spark.stop()
