from pyspark.sql import SparkSession
from pyspark.sql.functions import array_except
from pyspark.sql.types import StringType,StructType,StructField,IntegerType, ArrayType

# Initialize SparkSession
spark = SparkSession.builder.master("local[*]").appName("Krishna").getOrCreate()

# Sample DataFrame
data = [("Krishna", [1, 2, 3, 4,7],[7,3,8,9]),
        ("Kuldeep", [5, 6, 7, 8],[6,2,3]),
        ("Basheer", [1, 2, 3, 4, 5],[0,7,2,5])]
schema=StructType([
    StructField("name",StringType(),True),
    StructField("num_1",ArrayType(IntegerType()),True),
    StructField("num_2",ArrayType(IntegerType()),True)
])

new_df = spark.createDataFrame(data=data,schema=schema)

new_df.show()

# array except between num1 - num2
new_df.withColumn(" num1 except num2",array_except("num_1","num_2")).show()

# array except  between num2 - num1
new_df.withColumn("num2 except num1",array_except("num_2","num_1")).show()