
'''You will create a PySpark application named: assignment_04.py
Read the employees table into a DataFrame
Display the count of the number of records in the DF
Display the schema of the Employees Table from the DF
Create a DataFrame of the top 10,000 employee salaries (sort DESC) from the salaries table
Write the DataFrame back to database to a new table called: aces
Write the DataFrame out to the local system as a CSV and save it to local system using snappy compression (see the CSV chart in Chapter 04)
Use Native Pyspark file methods
'''

from pyspark.sql import SparkSession
from pyspark.sql.functions import when,current_date,col
# Create SparkSession
spark = SparkSession.builder \
           .appName('MySQLSpark') \
           .config("spark.jars", "mysql-connector-java-8.0.33.jar") \
           .getOrCreate()

# Read table using jdbc()
df = spark.read \
    .jdbc("jdbc:mysql://localhost:3306/employees", "employees", \
          properties={"user": "root", "password": "mysql", "driver":"com.mysql.cj.jdbc.Driver"})



record_count = df.count()
print("Number of records in the DataFrame:", record_count)
df.printSchema()
salary_df = spark.read \
    .jdbc("jdbc:mysql://localhost:3306/employees", "salaries", \
          properties={"user": "root", "password": "mysql", "driver":"com.mysql.cj.jdbc.Driver"})


top_salaries_df = salary_df.orderBy(salary_df["salary"].desc()).limit(10000)

result_table_name = 'aces'
mysql_url = "jdbc:mysql://localhost:3306/employees"
mysql_properties = {
    "user": "root",
    "password": "mysql",
    "driver": "com.mysql.cj.jdbc.Driver"
}
top_salaries_df.write.jdbc(mysql_url, result_table_name, mode="overwrite", properties=mysql_properties)

output_path = "output"
top_salaries_df.write \
    .format("csv") \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .save(output_path)

query = "(SELECT * FROM titles WHERE title = 'Senior Engineer') AS titles"
senior_df = spark.read.jdbc(url=mysql_url, table=query, properties=mysql_properties)


pyspark_df = senior_df.withColumn("employment_status",
                   when(col("to_date") == "9999-01-01", "current")
                   .otherwise(when(col("to_date") < current_date(), "left")))

pyspark_df.show()
left_count = pyspark_df.filter(col("employment_status") == "left").count()
current_count = pyspark_df.filter(col("employment_status") == "current").count()

print("Number of senior engineers who have left:", left_count)
print("Number of senior engineers who are currently employed:", current_count)


left_df = pyspark_df.filter(col("employment_status") == "left")
left_df.createOrReplaceTempView("left_tempview")
df_temp= spark.sql("select * from left_tempview")
df_temp.write \
    .format("jdbc") \
    .option("url", mysql_url) \
    .option("dbtable", "left_tempview") \
    .option("user", mysql_properties['user']) \
    .option("password", mysql_properties['password']) \
    .option("driver", mysql_properties['driver']) \
    .mode("overwrite") \
    .save()

left_df.write \
    .format("jdbc") \
    .option("url", mysql_url) \
    .option("dbtable", "left_table") \
    .option("user", mysql_properties['user']) \
    .option("password", mysql_properties['password']) \
    .option("driver", mysql_properties['driver']) \
    .mode("overwrite") \
    .save()


left_df.write\
    .format("jdbc") \
    .option("url", mysql_url) \
    .option("dbtable", "left_df") \
    .option("user", mysql_properties['user']) \
    .option("password", mysql_properties['password']) \
    .option("driver", mysql_properties['driver']) \
    .mode("overwrite") \
    .save()

spark.stop()



