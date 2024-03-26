from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

# Initialize SparkSession
spark = SparkSession.builder.appName("Krishna").getOrCreate()

# Create a DataFrame
data = [("Krishna",22),("Kuldeep",23),("Ravi",24),("Amit",21)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)

# df.show()

new_df=df.withColumn("CTC", lit(25000))
new_df.show()