from pyspark.sql import SparkSession
from pyspark.sql.functions import when

spark=SparkSession.builder.master("local[1]").appName("Krishna").getOrCreate()

emp_data = [("Krishna","Sales","UK",90000,23,"M"),
    ("Kuldeep","Sales","KA",86000,22,"M"),
    ("Basheer","Sales","TN",81000,24,"M"),
    ("Pratibha","Finance","MP",90000,21,"F"),
    ("Kalai","Finance","TN",99000,24,""),
    ("Manvi","Finance","RJ",83000,21,""),
    ("Ravi","Finance","UK",79000,25,"M"),
    ("Amit","Marketing","WB",80000,22,""),
    ("Pankaj","Marketing","DL",91000,26,"M")
  ]
schema = ["employee_name","department","state","salary","age","gender"]


df = spark.createDataFrame(data=emp_data, schema = schema)

new_df= df.withColumn("new_gender",when(df.gender=="M","Male")
                      .when(df.gender=="F","Female")
                      .otherwise("null"))

new_df.show()
