from pyspark.sql import SparkSession
from pyspark.sql.functions import *
# Initialize SparkSession
spark = SparkSession.builder.master("local[1]").appName("Krishna").getOrCreate()

# Create a DataFrame with sample data
data = [[1, "2024-03-30"], [2, "2024-03-31"], [3, "2024-04-01"]]
df = spark.createDataFrame(data, ["id", "date"])

# Show the initial DataFrame
df.show()


# Format date
formatted_date_df = df.select(date_format(df["date"], "dd-MM-yyyy").alias("formatted_date"))
formatted_date_df.show()

