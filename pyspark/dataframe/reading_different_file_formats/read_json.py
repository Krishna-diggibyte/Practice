from pyspark.sql import SparkSession
from pyspark.sql.types import StructType ,StructField ,StringType ,IntegerType

sc=SparkSession.builder.master("local").appName("Krishna").getOrCreate()

schema=StructType([
    StructField("age",IntegerType() ,True),
    StructField("emp_id",StringType(),True),
    StructField("name",StringType(),True)
])


json_df=sc.read.json("emp_data.json" , schema=schema)

json_df.printSchema()

json_df.show()

df_with_option = sc.read.option("header", True).json("emp_data.json")

df_with_option.printSchema()
df_with_option.show()



# json_df.write.mode('Overwrite').json("new_data.json")
# json_df.write.mode('Overwrite').format('json').save('new_data.json')