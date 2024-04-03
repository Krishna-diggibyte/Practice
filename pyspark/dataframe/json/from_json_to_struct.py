from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StringType,StructField
spark=SparkSession.builder.master("local[*]").appName("Krishna").getOrCreate()


data = [("Krishna", '{"age": 23, "city": "New Delhi"}'),
        ("Kuldeep", '{"age": 40, "city": "Karnataka"}')]
schema = ["name", "props"]
first_df = spark.createDataFrame(data, schema)
print(" original data and print schema of it")
first_df.show(truncate=False)
first_df.printSchema()

# json string to StructType
struct_schema = StructType([
        StructField("age",StringType(),True),
        StructField("city",StringType(),True)
])

struct_df = first_df.withColumn("propsStruct", from_json("props", struct_schema))

print("after convert into struct")
struct_df.show(truncate=False)
struct_df.printSchema()

print("extracting data from StructType")

result_df=struct_df.withColumn("age",struct_df.propsStruct.age).withColumn("city",struct_df.propsStruct.city).drop("props").drop("propsStruct")
result_df.show(truncate=False)