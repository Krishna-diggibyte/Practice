from pyspark.sql import SparkSession
from pyspark.sql.functions import array, array_contains, array_size, array_position, array_remove, lit, col
from pyspark.sql.types import StringType,StructType,StructField,IntegerType, ArrayType

# Initialize SparkSession
spark = SparkSession.builder.master("local[*]").appName("Krishna").getOrCreate()

# Sample DataFrame
data = [("Krishna", [1, 2, 3, 4,7]),
        ("Kuldeep", [5, 6, 7, 8]),
        ("Basheer", [1, 2, 3, 4, 5])]
schema=StructType([
    StructField("name",StringType(),True),
    StructField("numbers",ArrayType(IntegerType()),True)
])

new_df = spark.createDataFrame(data=data,schema=schema)

# Using ARRAY_CONTAINS function to check if an array contains a specific element
new_df.withColumn("Contains3", array_contains("Numbers", 3)).show()
