from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, trim

spark=SparkSession.builder.master("local[*]").appName("Krishna").getOrCreate()

# Sample DataFrame
data = [("   Krishna  ",25000), ("Kuldeep",26000), ("    Basheer  ",24000)]
df = spark.createDataFrame(data, ["name","salary"])

print("original data")
df.show()

def to_clean_str(df):
    return df.withColumn("cleand name",trim(upper(df.name)))

def double_salary(df):
    return df.withColumn("new salary",df.salary*2)

transformed_df = df.transform(to_clean_str).transform(double_salary).drop("salary")
transformed_df.show(truncate=False)
