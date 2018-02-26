import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * 在本地运行很慢，要几分钟才能出结果
  */
object DeviceDataExample {

  case class DeviceData(device: String, deviceType: String, signal: Double, time: String)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("DeviceDataExample")
      .getOrCreate()

    import spark.implicits._
    // Create DataFrame representing the stream of input lines from connection to localhost:9999

    val schema = new StructType().add("device", "string").add("deviceType", "string").add("signal", "double").add("time", "string")
    val df: DataFrame = spark.readStream
      .format("json")
      .schema(schema)
      .option("path", "input/devices")
      .load()

    val ds: Dataset[DeviceData] = df.as[DeviceData] // streaming Dataset with IOT device data

    // Select the devices which have signal more than 10
    df.select("device").where("signal > 10") // using untyped APIs
    ds.filter(_.signal > 10).map(_.device) // using typed APIs

    // Running count of the number of updates for each device type
    val deviceTypeCounts = df.groupBy("deviceType").count() // using untyped API

    // Running average signal for each device type
    import org.apache.spark.sql.expressions.scalalang.typed
    ds.groupByKey(_.deviceType).agg(typed.avg(_.signal)) // using typed API

    // Start running the query that prints the running counts to the console
    val query = deviceTypeCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()
  }

}
