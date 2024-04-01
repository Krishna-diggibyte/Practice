from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark=SparkSession.builder.master("local[1]").appName("Krishna").getOrCreate()

# Create a DataFrame with sample data
data = [[1, "2024-03-30"], [2, "2024-03-31"], [3, "2024-04-01"]]
df = spark.createDataFrame(data, ["id", "date"])

# Show the initial DataFrame
df.show()


# Get the current date
current_date_df = spark.sql("SELECT current_date() as current_date")
current_date_df.show()

         # or

df.select(current_date().alias("current_date")).show(1)


# Get the current timestamp

current_timestamp_df = spark.sql("SELECT current_timestamp() as current_timestamp")
current_timestamp_df.show(truncate=False)
         # or
df.select(current_timestamp().alias("current_timestamp")).show(1, truncate=False)

