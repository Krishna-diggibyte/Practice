from pyspark.sql import SparkSession

spark=SparkSession.builder.master("local[*]").appName("Krishna").getOrCreate()

data = [("Krishna",5,2,6),
        ("Kuldeep",6,3,0),
        ("pratibha",6,4,6)]


df = spark.createDataFrame(data, ["name","book_1","pen_1","page_1"])
print("orignal data")
df.show()

# Unpivot the DataFrame
unpivoted_df = df.selectExpr("name", "stack(3, 'book', book_1, 'pen', pen_1, 'page', page_1) as (product, value)")

# Show the result
unpivoted_df.show()
