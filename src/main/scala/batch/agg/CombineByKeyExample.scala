package batch.agg

import org.apache.spark.{SparkConf, SparkContext}

object CombineByKeyExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName)
    conf.set("spark.eventLog.enabled", "true")
    conf.set("spark.eventLog.dir", "file:/tmp/spark-events")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(Seq(("A", 110), ("A", 130), ("A", 120), ("B", 200), ("B", 206), ("B", 206), ("C", 150), ("C", 160), ("C", 170)))

    val createScoreCombiner = (score: Int) => (1, score)

    val scoreCombiner = (collector: (Int, Int), score: Int) => {
      val (numberScores, totalScore) = collector
      (numberScores + 1, totalScore + score)
    }

    val scoreMerger = (collector1: (Int, Int), collector2: (Int, Int)) => {
      val (numScores1, totalScore1) = collector1
      val (numScores2, totalScore2) = collector2
      (numScores1 + numScores2, totalScore1 + totalScore2)
    }
    val agg_rdd = rdd.combineByKey(createScoreCombiner, scoreCombiner, scoreMerger)

    val sum = agg_rdd.mapValues(x => (x._2 / x._1))
    sum.foreach(println)
  }
}
