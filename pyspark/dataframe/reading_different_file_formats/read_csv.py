from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

sc=SparkSession.builder.master("local").appName("Krishna").getOrCreate()


schema=StructType([
    StructField("emp_id",StringType(),True),
    StructField("name",StringType(),True),
    StructField("age",IntegerType() ,True)
])
# with schema
df_with_schema=sc.read.csv("emp_data2.csv",schema=schema)

#with inferschema
df_with_header=sc.read.csv("emp_data.csv",header=True,inferSchema=True)

#without schema
df_with_option = sc.read.option("header", True).csv("emp_data.csv")

#withschema
df_with_option_schema=sc.read.schema(schema).option("header",False).csv("emp_data2.csv")


df_with_schema.printSchema()
df_with_schema.show(truncate=False)
df_with_header.printSchema()
df_with_header.show(truncate=False)

df_with_option.printSchema()
df_with_option.show()

df_with_option_schema.printSchema()
df_with_option_schema.show()
