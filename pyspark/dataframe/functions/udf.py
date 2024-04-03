from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

spark=SparkSession.builder.master("local[*]").appName("Krishna").getOrCreate()

# Sample DataFrame
data = [("Krishna", 23), ("Kuldeep", 28), ("Basheer", 35)]
df = spark.createDataFrame(data, ["name", "age"])

# Define a UDF to double the age
def double_age(age):
    return age * 2

# Register the UDF
double_age_udf = udf(double_age, IntegerType())

# Apply the UDF to the DataFrame
df = df.withColumn("doubled_age", double_age_udf(df["age"]))

# Show the result
df.show()
