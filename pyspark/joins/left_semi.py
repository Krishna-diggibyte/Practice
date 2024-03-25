from pyspark.sql import SparkSession

sc= SparkSession.builder.master("local").appName("Krishna").getOrCreate()

data = [(1,"Krishna","40","M"), \
    (2,"Basheer","20","M"), \
    (3,"Kuldeep","10","M"), \
    (4,"Pratibhia","10","F"), \
    (5,"Manvi","40",""), \
      (6,"Amit","50","") \
  ]
column = ["emp_id","name","emp_dept_id","gender"]

empDF = sc.createDataFrame(data=data, schema = column)
dept = [("Full stack",10), \
    ("Data Science",20), \
    ("HR",30), \
    ("Data Engineer",40) \
  ]
deptColumns = ["dept_name","dept_id"]
deptDF = sc.createDataFrame(data=dept, schema = deptColumns)

# Print indivisual table
# empDF.show(truncate=False)
# deptDF.show(truncate=False)

# left semi join

empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"leftsemi").show(truncate=False)