from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("Krishna").getOrCreate()

name=spark.sparkContext.parallelize(["Krishna","Basheer","Kuldeep","Pratibha","Pankaj"])

print(name.collect())

grp_rdd=name.groupBy(lambda x: x[0])
print(grp_rdd.collect())

for (k,v) in grp_rdd.collect():
    print(k,list(v))