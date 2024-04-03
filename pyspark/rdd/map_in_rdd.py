from pyspark.sql import SparkSession

sc = SparkSession.builder.master("local[1]").appName("Krishna").getOrCreate()

# Create two RDD
rdd1 = sc.sparkContext.parallelize([1, 2, 3, 4, 5])
rdd2 = sc.sparkContext.parallelize([1, 2, 3, 4, 5], 2)  # 2 partitions [1,2][3,4,5]

# Apply map operation to double each element
doubled_rdd = rdd1.map(lambda x: x * 2)

# Apply mappartition operation to double each element
def f(iterator): yield sum(iterator)
map_rdd = rdd2.mapPartitions(f).glom().collect()
print(map_rdd)

# Apply mapPartitionsWithIndex operation to double each element in each partition
mapped_rdd = rdd2.mapPartitionsWithIndex(lambda index, partition: ((index, x * 2) for x in partition))

# Print the result
print(doubled_rdd.collect())

print(mapped_rdd.collect())
