from pyspark.sql import SparkSession
from pyspark.sql.functions import mean, avg, collect_list, collect_set, countDistinct, count, first, last

# Initialize SparkSession
spark = SparkSession.builder.master("local[1]").appName("Krishna").getOrCreate()


read_df = spark.read.csv("../../resource/emp_data.csv", inferSchema=True, header=True)


# Show the initial DataFrame
read_df.show()


# Get the first value
first_df = read_df.groupby("name").agg(first("age").alias("first_age"))
first_df.show()


# Get the last value
last_df = read_df.groupby("name").agg(last("age").alias("last_age"))
last_df.show()

