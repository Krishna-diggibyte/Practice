from pyspark.sql import SparkSession
from pyspark.sql.functions import *
# Initialize SparkSession
spark = SparkSession.builder.master("local[1]").appName("Krishna").getOrCreate()

# Create a DataFrame with sample data
data = [[1, "2024-03-30"], [2, "2024-03-31"], [3, "2024-04-01"]]
df = spark.createDataFrame(data, ["id", "date"])

# Show the initial DataFrame
df.show()


# Get the difference in days from current date
days_diff_df = df.select(datediff(current_date(), df["date"]).alias("days_since_current_date"))
days_diff_df.show()


# Convert string to date
to_date_df = df.select(to_date(df["date"]).alias("formatted_date"))
to_date_df.show()
