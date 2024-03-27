from pyspark.sql import SparkSession
sc=SparkSession.builder.master("local").appName("Krishna").getOrCreate()

# Create an RDD with duplicate elements
rdd = sc.sparkContext.parallelize([1, 2, 2, 3, 4, 4, 5])

# Apply distinct operation to get unique elements
new_rdd = rdd.distinct()

# Print the result
print(new_rdd.collect())