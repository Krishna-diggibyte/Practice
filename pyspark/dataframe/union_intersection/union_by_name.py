from pyspark.sql import SparkSession
sc= SparkSession.builder.master("local[*]").appName("Krishna").getOrCreate()

data1 = [(1,"Krishna","40","M"), \
    (2,"Basheer","20","M"), \
    (3,"Kuldeep","10","M"), \
    (4,"Pratibhia","10","F") \
  ]
column = ["emp_id","name","emp_dept_id","gender"]

data2 = [("Krishna","40","M",1), \
    ("Manvi","40","",5), \
      ("Amit","50","",6) \
  ]
column2=["name","emp_dept_id","gender","emp_id"]

empDF1 = sc.createDataFrame(data=data1, schema = column)
empDF2 = sc.createDataFrame(data=data2, schema = column2)



# union_intersection
union_df=empDF1.union(empDF2)

#union_intersection by name
union_by_name_df=empDF1.unionByName(empDF2)

empDF1.printSchema()
empDF1.show()
empDF2.printSchema()
empDF2.show()

print("union_intersection and union_intersection by name")

union_df.show()
union_by_name_df.show()