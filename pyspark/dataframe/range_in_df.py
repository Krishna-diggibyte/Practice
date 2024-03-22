from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.appName("Krishna").getOrCreate()

# Create a DataFrame with a range of numbers
df = spark.range(5)

# Show the result
df.show()
