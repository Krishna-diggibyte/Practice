from pyspark.sql import SparkSession
sc= SparkSession.builder.master("local").appName("Krishna").getOrCreate()

data1 = [(1,"Krishna","40","M"), \
    (2,"Basheer","20","M"), \
    (3,"Kuldeep","10","M"), \
    (4,"Pratibhia","10","F") \
  ]
column = ["emp_id","name","emp_dept_id","gender"]

data2 = [(1,"Krishna","40","M"), \
    (5,"Manvi","40",""), \
      (6,"Amit","50","") \
  ]

empDF1 = sc.createDataFrame(data=data1, schema = column)
empDF2 = sc.createDataFrame(data=data2, schema = column)



# intersection

intersection_df=empDF1.intersect(empDF2)
intersection_df.show()