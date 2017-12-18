/**
  * Created by lenovo on 2016/11/18.
  */

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object WordCount {
  val default_out = "spark/word-count/output";

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: WordCount <file>")
      System.exit(1)
    }

    var outputPath = default_out
    if (args.length == 2) {
      outputPath = args(1)
    }

    val logFile = args(0)
    val conf = new SparkConf().setAppName(" Word Count")
    val sc = new SparkContext(conf)
    val wordCount = sc.textFile(logFile, 2).flatMap(_.split("\\s")).map(word => (word, 1)).reduceByKey(_ + _)
    //    wordCount.saveAsTextFile(outputPath)
    wordCount.take(10).foreach(println)
    sc.stop()
  }
}
