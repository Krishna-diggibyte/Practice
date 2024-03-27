import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StringType,BooleanType,DateType

spark = SparkSession.builder.appName('SparkByExamples.com') \
                    .getOrCreate()

data = [("Krishna",23,"04-03-2024","true","M",30000),
    ("Basheer",22,"04-03-2024","true","M",33000),
    ("Kuldeep",24,"04-03-2024","false","M",32000)
  ]

columns = ["firstname","age","jobStartDate","isGraduated","gender","salary"]
df = spark.createDataFrame(data = data, schema = columns)
df.show(truncate=False)
df.printSchema()

df2 = df.withColumn("age",col("age").cast(StringType())) \
    .withColumn("isGraduated",col("isGraduated").cast(BooleanType())) \
    .withColumn("jobStartDate",col("jobStartDate").cast(DateType()))
df2.printSchema()

df3=df.select(col('jobStartDate').cast('date'),
              col('salary').cast('string'))
df3.printSchema()