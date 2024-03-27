from pyspark.sql import SparkSession

sc=SparkSession.builder.master("local").appName("Krishna").getOrCreate()

json_df=sc.read.json("emp_data.json")

json_df.printSchema()

json_df.show()

df_with_option = sc.read.option("header", True).json("emp_data.json")

df_with_option.printSchema()
df_with_option.show()



# json_df.write.mode('Overwrite').json("new_data.json")
# json_df.write.mode('Overwrite').format('json').save('new_data.json')