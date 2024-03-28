from pyspark.sql import SparkSession
from pyspark.sql.types import StringType,StructType,IntegerType,StructField
from pyspark.sql.functions import col


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

read_df=spark.read.csv("inventory_records.csv",schema=schema)


print("Original Data")
new_df=read_df.fillna(value=0)
new_df.show()


          # is null
print("Check which product_id have null value")
read_df.filter(read_df.Product_id.isNull()).show()

print("direct with filter")
read_df.filter("Product_id is NULL").show()

read_df.filter(col("Product_name").isNull()).show()