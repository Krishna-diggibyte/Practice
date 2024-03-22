from pyspark.sql import SparkSession

sc=SparkSession.builder.master("local").appName("Krishna").getOrCreate()

# Create an RDD
rdd = sc.sparkContext.parallelize([1, 2, 3, 4, 5], 2)  # 2 partitions

# Apply glom operation to convert each partition into a list
glommed_rdd = rdd.glom()

# Print the result
print(glommed_rdd.collect())
print(type(glommed_rdd.collect()))
