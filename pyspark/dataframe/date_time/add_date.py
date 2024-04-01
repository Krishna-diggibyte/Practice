from pyspark.sql import SparkSession
from pyspark.sql.functions import *
# Initialize SparkSession
spark = SparkSession.builder.master("local[1]").appName("Krishna").getOrCreate()

# Create a DataFrame with sample data
data = [[1, "2024-03-30"], [2, "2024-03-31"], [3, "2024-04-01"]]
df = spark.createDataFrame(data, ["id", "date"])

# Show the initial DataFrame
df.show()


# Add 2 days to the date
date_add_df = df.select(date_add(df["date"], 2).alias("date_after_2_days"))
date_add_df.show()
