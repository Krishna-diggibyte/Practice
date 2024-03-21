from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("Krishna").getOrCreate()

#  Create Empty RDD in PySpark
emptyrdd=spark.sparkContext.emptyRDD()

#data
datalist1=[("Kuldeep",20000),("Krishna", 18000),("Pratibha",25000)]

# creating rdd by parallelize and with data
firstrdd= spark.sparkContext.parallelize(datalist1)

# converting rdd to data frame
firstrdd.toDF().show()
print(type(firstrdd))