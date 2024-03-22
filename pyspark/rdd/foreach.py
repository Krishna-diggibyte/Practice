from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Krishna").getOrCreate()

data = [('Krishna' , 34), ('Pratibha' , 25) , ('Basheer' , 28) , ('Kuldeep' , 56)]

rdd = spark.sparkContext.parallelize(data)

#ForeachAction : print each element of RDD

rdd.foreach(lambda x : print(x))


#topAction : print top age of the rdd

top_elements = rdd.map(lambda x : x[1]).top(2)

print("Top 2 elements:", top_elements)