from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import rank,dense_rank

spark=SparkSession.builder.master("local[*]").appName("Krishna").getOrCreate()

read_df=spark.read.csv("../../resource/emp_manage.csv",inferSchema=True,header=True)

win_spec=Window.partitionBy("department").orderBy("annual_salary")


# rank and dense rank

read_df.withColumn("rank",rank().over(win_spec)).withColumn("dense rank",dense_rank().over(win_spec)).show()
