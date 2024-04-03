from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, from_json
from pyspark.sql.types import StructType, StringType,StructField
spark=SparkSession.builder.master("local[*]").appName("Krishna").getOrCreate()


data = [("Krishna", {"age": 23, "city": "New Delhi"}),
        ("Kuldeep", {"age": 40, "city": "Karnataka"})]
schema = ["name", "props"]
first_df = spark.createDataFrame(data, schema)
print(" original data and print schema , it is mapType")
first_df.show(truncate=False)
first_df.printSchema()

# struct to json
new_df=first_df.withColumn("propString",to_json(first_df.props))
new_df.show(truncate=False)
new_df.printSchema()