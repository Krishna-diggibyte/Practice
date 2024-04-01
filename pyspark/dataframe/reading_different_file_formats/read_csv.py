from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

sc=SparkSession.builder.master("local").appName("Krishna").getOrCreate()


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

# with custom schema
df_with_schema=sc.read.csv("../../resource/inventory_records_without_header.csv",schema=schema)

#with inferschema
df_with_header=sc.read.csv("../../resource/inventory_records.csv",header=True,inferSchema=True)

#without schema with header false
df_with_option_hader_false = sc.read.option("header", False).csv("../../resource/inventory_records_without_header.csv")

#without schema with header true
df_with_option_header_true = sc.read.option("header", True).csv("../../resource/inventory_records_without_header.csv")

#with schema and header false
df_with_option_schema=sc.read.schema(schema).option("header",False).csv("../../resource/inventory_records_without_header.csv")


df_with_schema.printSchema()
df_with_schema.show(truncate=False)
df_with_header.printSchema()
df_with_header.show(truncate=False)

df_with_option_hader_false.printSchema()
df_with_option_hader_false.show()

df_with_option_header_true.printSchema()
df_with_option_header_true.show()

df_with_option_schema.printSchema()
df_with_option_schema.show()
