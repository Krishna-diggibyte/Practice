from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("Krishna").getOrCreate()

data=[("Krishna","Bhandari","M",50000),
      ("Kuldeep","Managoli" ,"M",50000),
      ("Pratibha","Nimbolkar","F",50000),
      ("Basheer","K","M",50000)]

columns=["first_name","last_name","gender","salary"]

pyspark_df1=spark.createDataFrame(data=data,schema=columns)
pyspark_df1.printSchema()
pyspark_df1.show(truncate=False)

pyspark_rdd=pyspark_df1.rdd
print(type(pyspark_df1))
print(type(pyspark_rdd))

datatt=pyspark_rdd.collect()
for row in datatt:
      print(row[0]+" , "+row[1]+" , "+row[2]+" , "+str(row[3]))