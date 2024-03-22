from pyspark.sql import SparkSession

sc=SparkSession.builder.master("local").appName("Krishna").getOrCreate()

# Create an RDD of key-value pairs
rdd = sc.sparkContext.parallelize([("Krishna","Football"),("Kuldeep","Basketball"),("Krishna","Vollyball"),("Amit","Chess")])

# Apply groupByKey operation to group values by key
grouped_rdd = rdd.groupByKey()

# Print the result
print(grouped_rdd.mapValues(list).collect())
