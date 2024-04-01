from pyspark.sql import SparkSession
from pyspark.sql.functions import mean, avg, collect_list, collect_set, countDistinct, count, first, last

# Initialize SparkSession
spark = SparkSession.builder.master("local[1]").appName("Krishna").getOrCreate()


read_df = spark.read.csv("../../resource/emp_data.csv", inferSchema=True, header=True)


# Show the initial DataFrame
read_df.show()


# Calculate the mean of Age
mean_df = read_df.select(mean(read_df["age"]).alias("mean_age"))
mean_df.show()


# Calculate the average of Age
avg_df = read_df.select(avg(read_df["age"]).alias("average_age"))
avg_df.show()


# Count distinct values
count_distinct_df = read_df.select(countDistinct("name").alias("distinct_names_count"))
count_distinct_df.show()


# Count total values
count_df = read_df.select(count("*").alias("total_records"))
count_df.show()

