from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import *

spark=SparkSession.builder.master("local[1]").appName("Krishna").getOrCreate()

read_df=spark.read.csv("../../resource/emp_manage.csv",header=True,inferSchema=True)

read_df.printSchema()
read_df.show()

window_spec=Window.partitionBy("department").orderBy("annual_salary")

# row number
read_df.withColumn("row number",row_number().over(window_spec)).show()
