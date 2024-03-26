from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

# Initialize SparkSession
spark = SparkSession.builder.appName("Krishna").getOrCreate()

# Create a DataFrame
data = [("Krishna",22,25000),("Kuldeep",23,24999),("Ravi",24,26000),("Amit",21,25000)]
columns = ["Name", "Age","CTC"]
df = spark.createDataFrame(data, columns)

df.show()
renamed_df=df.withColumnRenamed("CTC","salary")
renamed_df.show()