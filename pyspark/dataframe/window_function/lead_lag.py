from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import lead,lag

spark=SparkSession.builder.master("local[*]").appName("Krishna").getOrCreate()

read_df=spark.read.csv("../../resource/emp_manage.csv",inferSchema=True,header=True)

win_spec=Window.partitionBy("department").orderBy("annual_salary")

read_df.withColumn("lead",lead("annual_salary",2).over(win_spec)).withColumn("lag",lag("annual_salary",2).over(win_spec)).show()