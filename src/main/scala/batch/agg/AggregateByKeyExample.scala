package batch.agg

import org.apache.spark.{SparkConf, SparkContext}

object aggregateByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName)
    conf.set("spark.eventLog.enabled", "true")
    conf.set("spark.eventLog.dir", "file:/tmp/spark-events")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(Seq((2, 110), (2, 130), (2, 120), (3, 200), (3, 206), (3, 206), (4, 150), (4, 160), (4, 170)))
    val agg_rdd = rdd.aggregateByKey((0, 0))((acc, value) => (acc._1 + value, acc._2 + 1), (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
    val sum = agg_rdd.mapValues(x => (x._1 / x._2))
    sum.foreach(println)
  }
}
