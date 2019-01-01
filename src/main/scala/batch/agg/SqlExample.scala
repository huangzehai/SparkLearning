package batch.agg

import org.apache.spark.sql.SparkSession

object SqlExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getSimpleName)
      .config("spark.eventLog.enabled", "true")
      .config("spark.eventLog.dir", "file:/tmp/spark-events")
//      .config("spark.default.parallelism","23")
      .getOrCreate()
    val df = spark.read.format("csv")
      .option("header", "true")
      .load("file:/Users/huangzehai/data/ml-latest/ratings.csv")
    import spark.implicits._
    df.createTempView("ratings")
    val avgDf =spark.sql("select movieId, avg(rating) from ratings group by movieId")
    avgDf.rdd.saveAsTextFile("output/sql/avgRatings")

  }

}


