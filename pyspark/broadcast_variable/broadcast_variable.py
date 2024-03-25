from pyspark.sql import SparkSession
sc=SparkSession.builder.master("local").appName("Krishna").getOrCreate()


# Create an RDD
data = [1, 2, 3, 4, 5]
rdd = sc.sparkContext.parallelize(data)

# Create a broadcast variable
broadcast_var = sc.sparkContext.broadcast([10, 20, 30])

# Use the broadcast variable in a transformation
result_rdd = rdd.map(lambda x: x + broadcast_var.value[0])

# Collect the results
result = result_rdd.collect()

# Print the results
print("Original data:", data)
print("Broadcast variable:", broadcast_var.value)
print("Result after adding broadcast variable:", result)

# Stop the Spark session
sc.stop()
