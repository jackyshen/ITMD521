from pyspark.sql import SparkSession
import sys
from pyspark.sql.functions import col, desc
from pyspark.sql.functions import when
from pyspark.sql.functions import to_date
from pyspark.sql.functions import month
from pyspark.sql.functions import dayofmonth

spark = (SparkSession.builder
         .appName("Lab3") \
         .config("spark.sql.legacy.allowNonEmptyLocationInCTAS", "true") \
        .getOrCreate())
file_path = sys.argv[1]
#df = spark.read.csv(file_path,header=True,inferSchema=True)
df = spark.read.format("csv")\
        .option("header","true")\
        .load(file_path)
df.show()

df.createOrReplaceTempView("us_delay_flights_tbl")

spark.sql("""SELECT date, delay, origin, destination
FROM us_delay_flights_tbl
WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD'
ORDER by delay DESC""").show(10)


spark.sql("""SELECT delay, origin, destination,
CASE
WHEN delay > 360 THEN 'Very Long Delays'
WHEN delay > 120 AND delay < 360 THEN 'Long Delays'
WHEN delay > 60 AND delay < 120 THEN 'Short Delays'
WHEN delay > 0 and delay < 60 THEN 'Tolerable Delays'
WHEN delay = 0 THEN 'No Delays'
ELSE 'Early'
END AS Flight_Delays
FROM us_delay_flights_tbl
ORDER BY origin, delay DESC""").show(10)



(df.select("date", "delay", "origin","destination")
.filter((col("delay") > 120) & (col("origin") == "SFO") & (col("destination") == "ORD"))
.orderBy(col("delay").desc())).show(10)


(df.select("delay", "origin", "destination",
            when(col("delay") > 360, "Very Long Delays")
            .when((col("delay") > 120) & (col("delay") < 360), "Long Delays")
            .when((col("delay") > 60) & (col("delay") < 120), "Short Delays")
            .when((col("delay") > 0) & (col("delay") < 60), "Tolerable Delays")
            .when(col("delay") == 0, "No Delays")
            .otherwise("Early")
            .alias("Flight_Delays")) \
    .orderBy(col("origin"), col("delay").desc()))

spark.sql("DROP DATABASE IF EXISTS learn_spark_db CASCADE")


spark.sql("CREATE DATABASE learn_spark_db")

df = spark.read.csv(file_path, header=True)
df.select(col("date"),to_date( col("date"), 'yyyy-MM-dd HH:mm')).collect()




#df = df.withColumn("date", to_date(df["date"],'yyyyMMddHHmm'))
df.show(30)
#spark.sql("DROP TABLE us_delay_flights_tbl")
#spark.sql("DROP TABLE IF EXISTS us_delay_flights_tbl")
#df.write.mode("overwrite").saveAsTable("us_delay_flights_tbl")

result = spark.sql("""
    SELECT *
    FROM us_delay_flights_tbl 
    WHERE origin = 'ORD'
""")
result.show(14)
df.createOrReplaceTempView("flights_view")



#AND to_date(date, 'MM/dd') BETWEEN to_date('03/01', 'MM/dd') AND to_date('03/15', 'MM/dd')
spark.sql("""
    CREATE OR REPLACE TEMP VIEW  tempView AS
    SELECT *
    FROM flights_view
    WHERE origin = 'ORD'
    AND MONTH(date) = 3
    AND DAY(date) BETWEEN 1 AND 15
""")
result = spark.sql("""SELECT date_format(date,'yyyy-MM-dd HH:mm') AS new_date,delay, distance, origin, destination FROM tempView""")
result.show(5)

(df
.write
.mode("overwrite")
.option("path", file_path)
.saveAsTable("us_delay_flights_tbl"))


spark.catalog.listColumns("us_delay_flights_tbl")

#df_ord = spark.sql("SELECT  date  , delay, distance, origin ,destination  FROM us_delay_flights_tbl WHERE origin = 'ORD' ")

#df_ord.createOrReplaceTempView("us_origin_airport_ORD_tmp_view")
#spark.table("us_origin_airport_ORD_tmp_view").show(5)


## part3 



df = spark.read.csv(file_path, header=True)
df.select(col("date"),to_date( col("date"), 'yyyy-MM-dd HH:mm')).collect()

df.write.mode("overwrite").json("departuredelays.json")
df.write.mode("overwrite").format("json").option("compression", "org.apache.hadoop.io.compress.Lz4Codec").save("departuredelays.lz4")

#df.write.mode("overwrite").option("compression", "org.apache.spark.io.LZ4CompressionCodec").json("departuredelays.json")
df.write.mode("overwrite").parquet("departuredelays.parquet")



## part4




df = spark.read.parquet("departuredelays.parquet")

df.filter(df["origin"] == "ORD").show(10)

df.write.mode("overwrite").parquet("orddeparturedelays")



#part 4 

spark.stop()
