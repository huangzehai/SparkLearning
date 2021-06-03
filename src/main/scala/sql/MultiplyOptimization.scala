//package sql
//
//import org.apache.spark.sql.SparkSession
//
//
//object MultiplyOptimization {
//  def main(args: Array[String]): Unit = {
//    val spark = SparkSession.builder().appName(this.getClass.getSimpleName)
//      .config("spark.eventLog.enabled", "true")
//      .config("spark.eventLog.dir", "file:/tmp/spark-events")
//      .getOrCreate()
//    import spark.implicits._
//    spark.sqlContext.experimental.extraOptimizations = Seq(MultiplyOptimizationRule)
//    val df = spark.range(20000000).selectExpr("id * 1")
//    println(df.queryExecution.optimizedPlan.numberedTreeString)
//
//  }
//
//}
