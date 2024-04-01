from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.master("local[1]").appName("Krishna").getOrCreate()

read_df = spark.read.csv("../../resource/emp_data.csv", inferSchema=True, header=True)

read_df.show()

#    and operation
and_df = read_df.filter(read_df.name.like("K%") & (read_df.age <= 24))
and_df.show()

#    or operation
or_df=read_df.filter(read_df.name.like("K%") | (col("age") <= 24))
or_df.show()

#    not operation
not_df=read_df.filter(~read_df.name.like("K%"))
not_df.show()
