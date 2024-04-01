from pyspark.sql import SparkSession
from pyspark.sql.functions import *
# Initialize SparkSession
spark = SparkSession.builder.master("local[1]").appName("Krishna").getOrCreate()

# Create a DataFrame with sample data
data = [[1, "2024-03-30"], [2, "2024-03-31"], [3, "2024-04-01"]]
df = spark.createDataFrame(data, ["id", "date"])

# Show the initial DataFrame
df.show()


# Extract year, month, and day from date
year_month_day_df = df.select(year(df["date"]).alias("year"),
                              month(df["date"]).alias("month"),
                              dayofmonth(df["date"]).alias("day_of_month"))
year_month_day_df.show()

