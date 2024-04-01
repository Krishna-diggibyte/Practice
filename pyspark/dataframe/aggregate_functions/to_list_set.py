from pyspark.sql import SparkSession
from pyspark.sql.functions import mean, avg, collect_list, collect_set, countDistinct, count, first, last

# Initialize SparkSession
spark = SparkSession.builder.master("local[1]").appName("Krishna").getOrCreate()


read_df = spark.read.csv("../../resource/emp_data.csv", inferSchema=True, header=True)


# Show the initial DataFrame
read_df.show()


# Collect values into a list
collect_list_df = read_df.groupby("name").agg(collect_list("age").alias("ages_list"))
collect_list_df.show(truncate=False)


# Collect values into a set
collect_set_df = read_df.groupby("name").agg(collect_set("age").alias("ages_set"))
collect_set_df.show(truncate=False)

