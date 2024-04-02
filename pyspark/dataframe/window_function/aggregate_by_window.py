from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import avg, sum, col

spark=SparkSession.builder.master("local[*]").appName("Krishna").getOrCreate()

read_df=spark.read.csv("../../resource/emp_manage.csv",inferSchema=True,header=True)

win_spec=Window.partitionBy("department")

# avg  with select query
read_df.withColumn("avg",avg(col=("annual_salary")).over(win_spec)).select("full_name","department","annual_salary","avg").show()

# sum with all data
read_df.withColumn("sum",sum(col=("annual_salary")).over(win_spec)).show()