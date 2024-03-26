from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.appName("Krishna").getOrCreate()

# Create a DataFrame
data = [("Krishna",22),("Kuldeep",23),("Ravi",24),("Amit",21),("Basheer",23)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)

# only print select data
df.select("Name").show()

# Select All columns from List
df.select(*columns).show()


