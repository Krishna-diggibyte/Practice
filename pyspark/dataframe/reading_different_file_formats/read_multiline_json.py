from pyspark.sql import SparkSession
from pyspark.sql.types import StructType ,StructField ,StringType ,IntegerType

spark=SparkSession.builder.master("local[1]").appName("Krishna").getOrCreate()

schema=StructType([
    StructField("name",StringType(),True),
    StructField("phoneNumber",StringType(),True),
    StructField("email",StringType(),True),
    StructField("address",StringType(),True),
    StructField("userAgent",StringType(),True),
    StructField("hexcolor",StringType(),True)
])

read_json=spark.read.json("../../resource/multiline.json",schema=schema,multiLine=True)

read_json.printSchema()

read_json.show(truncate=False)