from pyspark.sql import SparkSession

spark=SparkSession.builder.master("local[*]").appName("Krishna").getOrCreate()

# Sample DataFrame
data = [("Krishna",  "book",5),
        ("Kuldeep", "book",6),
        ("pratibha", "book",6),
        ("Krishna", "pen",2),
        ("Kuldeep", "pen",3,),
        ("pratibha", "pen",4),
        ("Krishna", "page",5),
        ("pratibha", "book",6)]


df = spark.createDataFrame(data, ["name","product", "value"])
print("orignal data")
df.show()

# Pivot the DataFrame
pivoted_df = df.groupBy("name").pivot("product").sum("value")
print("after pivot")
# Show the result
pivoted_df.show()

