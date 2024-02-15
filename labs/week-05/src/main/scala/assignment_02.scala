package main.scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.Encoders


object Lab3{
def main(args: Array[String]) {

// 创建一个 SparkSession
val spark = SparkSession.builder
  .appName("Lab3")
  .getOrCreate()

case class DeviceIoTData (battery_level: Long, c02_level: Long,
cca2: String, cca3: String, cn: String, device_id: Long,
device_name: String, humidity: Long, ip: String, latitude: Double,
lcd: String, longitude: Double, scale:String, temp: Long,
timestamp: Long)

val ds  = spark.read.json("./data/iot_devices.json")
val ds = spark.read.json("./data/iot_devices.json").as[DeviceIoTData]


ds.show(5,false)
 val schema = new StructType()
      .add("battery_level", LongType, true)
      .add("c02_level", LongType, true)
      .add("cca2", StringType, true)
      .add("cca3", StringType, true)
      .add("cn", StringType, true)
      .add("device_id", LongType, true)
      .add("device_name", StringType, true)
      .add("humidity", LongType, true)
      .add("ip", StringType, true)
      .add("latitude", DoubleType, true)
      .add("lcd", StringType, true)
      .add("longitude", DoubleType, true)
      .add("scale", StringType, true)
      .add("temp", LongType, true)
      .add("timestamp", LongType, true)
    val df_with_schema = spark.read.schema(schema).json("./data/iot_devices.json")
df_with_schema.printSchema()
    df_with_schema.show(false)


     print("Enter an integer: ")
     val num = StdIn.readInt()
  df_with_schema.select("device_name").where(col("battery_level")< $num).show(false)


spark.stop()
}
}