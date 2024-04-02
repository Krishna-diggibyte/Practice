from pyspark.sql import SparkSession
from pyspark.sql.functions import col

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

# Print indivisual table
# empDF.show(truncate=False)

# self join
self_join = empDF.alias("emp1").join(empDF.alias("emp2"), col("emp1.emp_id") == col("emp2.emp_id"),"inner")

self_join.show()
