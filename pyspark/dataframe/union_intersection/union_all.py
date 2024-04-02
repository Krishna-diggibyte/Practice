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



# union_intersection all

union_all_df=empDF1.unionAll(empDF2)
union_all_df.show()

# union_intersection all without duplicates

empDF1.unionAll(empDF2).distinct().show()