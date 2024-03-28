from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark=SparkSession.builder.master("local[1]").appName("Krishna").getOrCreate()

schema=StructType([
    StructField("Emp_id",StringType(),True),
    StructField("Name",StringType(),True),
    StructField("Age",IntegerType() ,True)
])

df_read=spark.read.parquet("emp_data.parquet")

df_read.printSchema()
df_read.show()

# df_with_option = spark.read.option("header", False).parquet("emp_data.parquet")
#
# df_with_option.printSchema()
# df_with_option.show()



# df_read.write.format("csv").mode("overwrite").save("sample/emp_data1.csv")



