package batch.agg

import org.apache.spark.{SparkConf, SparkContext}

object GroupByKeyExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    conf.set("spark.eventLog.enabled", "true")
    conf.set("spark.eventLog.dir", "file:/tmp/spark-events")
    val sc = new SparkContext(conf)
    //    val scores = sc.parallelize(Seq(("A", 110), ("A", 130), ("A", 120), ("B", 200), ("B", 206), ("B", 206), ("C", 150), ("C", 160), ("C", 170)))
    val lines = sc.textFile("file:/Users/huangzehai/data/ml-latest/ratings.csv")
    val linesWithoutHead = lines.mapPartitionsWithIndex {
      (idx, iterator) => if (idx == 0) iterator.drop(1) else iterator
    }
    val ratings = linesWithoutHead.map(line => {
      val splits = line.split(",")
      (splits(1).toInt, splits(2).toDouble)
    })
    val avgRatings = ratings.groupByKey.mapValues { values => values.sum / values.size }
    avgRatings.saveAsTextFile("output/groupByKey/avgRatings")
  }

}
