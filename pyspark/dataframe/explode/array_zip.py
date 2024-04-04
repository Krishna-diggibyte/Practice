from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,ArrayType,BooleanType,LongType

spark=SparkSession.builder.master("local[*]").appName("Krishna").getOrCreate()


schema=StructType([
    StructField("employees",ArrayType(StructType([
        StructField("empId",IntegerType(),True),
        StructField("empName",StringType(),True)
    ]),True)),
    StructField("id",IntegerType(),True),
    StructField("properties",StructType([
        StructField("name",StringType(),True),
        StructField("storeSize",StringType(),True)
    ]))
])

read_df=spark.read.json("../../resource/sample_json_3.json",multiLine=True,schema=schema)

print("original json")
read_df.show(truncate=False)
# read_df.printSchema()

print("array zip")

zip_df=read_df.withColumn("zipped_value",f.arrays_zip("employees")).drop("employees")
zip_df.show(truncate=False)


print("explode")
exp_df=zip_df.withColumn("explode_value",f.explode("zipped_value")).drop("zipped_value")
exp_df.show(truncate=False)
exp_df.printSchema()

print("extracting values")
result_df=exp_df.withColumn("empId",exp_df.explode_value.employees['empId']).withColumn("empName",exp_df.explode_value.employees['empName']) \
    .withColumn("name",exp_df.properties['name']).withColumn("storeSize",exp_df.properties['storeSize']).drop("explode_value","properties")
result_df.show(truncate=False)

