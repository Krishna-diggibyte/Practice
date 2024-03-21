from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Krishna').getOrCreate()

Data = [1,2,3,4,6,5,8,7]

rdd_1 = spark.sparkContext.parallelize(Data)


# mean() - Calculate the arithmetic mean of elements in the RDD
mean_value = rdd_1.mean()
print("Mean value:", mean_value)

# min() - Find the minimum element in the RDD based on default ordering
min_value = rdd_1.min()
print("Min value:", min_value)

# max() - Find the maximum element in the RDD based on default ordering
max_value = rdd_1.max()
print("Max value:", max_value)

