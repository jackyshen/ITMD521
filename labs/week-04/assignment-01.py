# In python lab02
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = ( SparkSession
    .builder
    .appName("Lab2")
    .getOrCreate())
df = spark.read.csv("path/to/your/csv/file.csv", header=True)
df.printSchema()
print("\nNumber of Rows:", df.count())

schema = StructType([
    StructField("trip_id",StringType(),True),
    StructField("starttime",StringType(),True),
    StructField("stoptime",StringType(),True),
    StructField("bikeid",StringType(),True),
    StructField("tripduration",StringType(),True),
    StructField("from_station_id",StringType(),True),
    StructField("from_station_name",StringType(),True),
    StructField("to_station_id",StringType(),True),
    StructField("to_station_name",StringType(),True),
    StructField("usertype",StringType(),True),
    StructField("gender",StringType(),True),
    StructField("birthyear",StringType(),True),
])

df_with_schema = spark.createDataFrame(df.rdd,schema)

df_with_schema.show()
print("\nNumber of Rows:", df_with_schema.count())
spark.stop()
