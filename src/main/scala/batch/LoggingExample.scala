package batch

import org.slf4j.LoggerFactory

object LoggingExample {
  val logger = LoggerFactory.getLogger("LoggingExample")

  def main(args: Array[String]): Unit = {

    import org.apache.spark.sql.SparkSession

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    val lines = spark.read.textFile("input/movielens/movies.csv")
    logger.info("lines count: {}", lines.count())

  }

}
