import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.types.{DataTypes, TimestampType}

object EventTimeWindowWordCountExample {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("EventTimeWindowWordCountExample")
      .getOrCreate()


    import spark.implicits._
    val df = Seq(("Hello", "2018-02-24 16:35:09"), ("world", "2018-02-24 16:35:09"), ("Hello", "2018-02-24 16:35:09"), ("Adam", "2018-02-24 16:45:09")).toDF("word", "ts")
    import org.apache.spark.sql.functions.unix_timestamp
    val ts = unix_timestamp($"ts", "yyyy-MM-dd HH:mm:ss")
    //替换列
    df.withColumn("ts", ts)

    // Group the data by window and word and compute the count of each group
    import org.apache.spark.sql.functions._
    val windowedCounts = df.groupBy(window($"ts", "10 minutes", "2 minutes"),
      $"word"
    ).count()

    windowedCounts.collect().foreach(println)
  }

  case class Word(word: String, timestamp: TimestampType);

}
