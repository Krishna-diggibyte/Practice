from pyspark.sql import SparkSession

spark=SparkSession.builder.master("local[1]").appName("Krishna").getOrCreate()

emp_data = [("Krishna","Sales","UK",90000,23,10000),
    ("Kuldeep","Sales","KA",86000,22,20000),
    ("Basheer","Sales","TN",81000,24,23000),
    ("Pratibha","Finance","MP",90000,21,23000),
    ("Kalai","Finance","TN",99000,24,24000),
    ("Manvi","Finance","RJ",83000,21,19000),
    ("Ravi","Finance","UK",79000,25,15000),
    ("Amit","Marketing","WB",80000,22,18000),
    ("Pankaj","Marketing","DL",91000,26,21000)
  ]
schema = ["employee_name","department","state","salary","age","bonus"]

df = spark.createDataFrame(data=emp_data, schema = schema)

# df.show(truncate=False)

#  like
df.filter(df.employee_name.like("K%")).show(truncate=False)


