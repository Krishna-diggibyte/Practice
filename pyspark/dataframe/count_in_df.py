from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.appName("Krishna").getOrCreate()

# Create a DataFrame
data = [("Krishna",22),("Kuldeep",23),("Ravi",24),("Amit",21)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)

# count() - Returns the number of rows in the DataFrame
num_rows = df.count()

# Print number of rows
print("Number of Rows:", num_rows)

# Stop SparkSession
spark.stop()
