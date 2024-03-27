from pyspark.sql import SparkSession

spark=SparkSession.builder.master("local[1]").appName("Krishna").getOrCreate()

df_read=spark.read.parquet("emp_data.parquet")

df_read.printSchema()
df_read.show()

df_with_option = spark.read.option("header", True).parquet("emp_data.parquet")

df_with_option.printSchema()
df_with_option.show()



# df_read.write.format("csv").mode("overwrite").save("sample/emp_data1.csv")



