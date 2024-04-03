from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StringType ,MapType

spark=SparkSession.builder.master("local[*]").appName("Krishna").getOrCreate()


data = [("Krishna", '{"age": 23, "city": "New Delhi"}'),
        ("Kuldeep", '{"age": 40, "city": "Karnataka"}')]
schema = ["name", "props"]
first_df = spark.createDataFrame(data, schema)
print(" original data and print schema of it")
first_df.show(truncate=False)
first_df.printSchema()

# json string to Maptype
map_schema = MapType(StringType(),StringType())

mapped_df = first_df.withColumn("propsMap", from_json("props", map_schema))

print("after mapped")
mapped_df.show(truncate=False)
mapped_df.printSchema()

print("extracting data from mapType")

result_df=mapped_df.withColumn("age",mapped_df.propsMap.age).withColumn("city",mapped_df.propsMap.city).drop("props").drop("propsMap")
result_df.show(truncate=False)