
from __future__ import print_function
import sys
from pyspark.sql import SparkSession



if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: assignment_02 <file>", file=sys.stderr)
        sys.exit(-1)

    spark = (SparkSession
        .builder
        .appName("assignment_02")
        .getOrCreate())
    # get the M&M data set file name
    csv_file = sys.argv[1]
    # read the file into a Spark DataFrame
    df = (spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(mnm_file))
    df.show(10)
    df.printSchema()
    



    spark.stop()