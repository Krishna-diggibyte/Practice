from pyspark.sql import SparkSession

sc=SparkSession.builder.master("local").appName("Krishna").getOrCreate()

# Create two RDDs
rdd1 = sc.sparkContext.parallelize(["Krishna", "Amit"])
rdd2 = sc.sparkContext.parallelize(["Pratibha","Kuldeep","Krishna"])

# Apply intersection operation to find common elements
new_rdd= rdd1.intersection(rdd2)

# Print the result
print(new_rdd.collect())