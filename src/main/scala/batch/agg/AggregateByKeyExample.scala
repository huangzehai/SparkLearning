package batch.agg

import org.apache.spark.{SparkConf, SparkContext}

object AggregateByKeyExample {
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
    val agg_rdd = ratings.aggregateByKey((0.0, 0))((acc, value) => (acc._1 + value, acc._2 + 1), (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
    val avgRatings = agg_rdd.mapValues(x => (x._1 / x._2))
    avgRatings.saveAsTextFile("output/aggregateByKey/avgRatings")
  }
}
