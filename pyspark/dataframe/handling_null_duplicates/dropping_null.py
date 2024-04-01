from pyspark.sql import SparkSession
from pyspark.sql.types import StringType,StructType,IntegerType,StructField

spark=SparkSession.builder.master("local[*]").appName("Krishna").getOrCreate()

schema =StructType([
    StructField("Product_id",StringType(),True),
    StructField("Product_name",StringType(),True),
    StructField("Opening_Stocks",IntegerType(),True),
    StructField("PurchaseStock in",IntegerType(),True),
    StructField("Number of Units Sold",IntegerType(),True),
    StructField("Hand-In-Stock",IntegerType(),True),
    StructField("Cost Price Per Unit (USD)",IntegerType(),True),
    StructField("Cost Price Total (USD)",IntegerType(),True)
])

read_df=spark.read.csv("../../resource/inventory_records.csv",schema=schema,header=True)

# read_df.printSchema()

print("Original data")
read_df.show()

print("if any cell is null")
read_df.na.drop().show()

print("If all row is null like P104")
read_df.na.drop(how="all").show(truncate=False)

print("check non null values by threshold")
read_df.na.drop(how="any",thresh=7).show(truncate=False)



print("If null in Opening Stocks")
read_df.na.drop(subset=["Opening_Stocks"]).show(truncate=False)
# also by not null

print("Check which Opening_Stocks have null value")
read_df.filter(read_df.Opening_Stocks.isNotNull()).show()


print("all null removed")
read_df.dropna().show(truncate=False)