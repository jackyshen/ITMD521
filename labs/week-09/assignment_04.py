
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
# Create SparkSession
spark = SparkSession.builder \
           .appName('MySQLSpark') \
           .config("spark.jars", "mysql-connector-java-8.0.33.jar") \
           .getOrCreate()

# Read table using jdbc()
df = spark.read \
    .jdbc("jdbc:mysql://localhost:3306/employees", "employees", \
          properties={"user": "root", "password": "mysql", "driver":"com.mysql.cj.jdbc.Driver"})


df.printSchema()
record_count = df.count()
print("Number of records in the DataFrame:", record_count)




