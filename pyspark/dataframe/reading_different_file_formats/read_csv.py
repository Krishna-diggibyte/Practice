from pyspark.sql import SparkSession

sc=SparkSession.builder.master("local").appName("Krishna").getOrCreate()

df_without_header=sc.read.csv("emp_data.csv")
df_with_header=sc.read.csv("emp_data.csv",header=True,inferSchema=True)
df_with_option = sc.read.option("header", True).csv("emp_data.csv")


df_without_header.printSchema()
df_without_header.show(truncate=False)
df_with_header.printSchema()
df_with_header.show(truncate=False)


df_with_option.printSchema()
df_with_option.show()
