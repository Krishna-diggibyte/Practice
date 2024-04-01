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

read_df=spark.read.csv("../../resource/inventory_records.csv",schema=schema)


print("Original Data")
new_df=read_df.fillna(value=0)
new_df.show(40)


print("Check which Opening_Stocks have Not-null value")
read_df.filter(read_df.Opening_Stocks.isNotNull()).show()

print("Replaced by nothing here on string")
read_df.na.fill(value="__nothing_here").show()

print("change value of product name and opening stock")
read_df.na.fill(value="__product_name",subset="Product_name").na.fill(value=0,subset="Opening_Stocks").show()

read_df.na.fill({"Product_name":"_product_name_","Opening_Stocks":1000}).show()
