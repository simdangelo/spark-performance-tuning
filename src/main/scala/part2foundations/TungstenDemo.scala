package part2foundations

import org.apache.spark.sql.SparkSession

object TungstenDemo {

  val spark = SparkSession.builder()
    .appName("Tungsten Demo")
    .master("local")
    .config("spark.sql.adaptive.enabled", "false")
    .getOrCreate()

  val sc = spark.sparkContext

  val numberRDD = sc.parallelize(1 to 10000000).cache() // cache saves this RDD in memory
  numberRDD.count()
  numberRDD.count() // much faster

  import spark.implicits._

  val numbersDF = numberRDD.toDF("values").cache() // DFs, unlike RDD, are cached with Tungsten
  numbersDF.count()
  numbersDF.count() // much faster

  // Tungsten is active in WholeStageCodgen

  spark.conf.set("spark.sql.codegen.wholeStage", "false")
  val noWholeStageSum = spark.range(10000000).selectExpr("sum(id)")
  noWholeStageSum.explain()
  noWholeStageSum.show()

  spark.conf.set("spark.sql.codegen.wholeStage", "true")
  val WholeStageSum = spark.range(10000000).selectExpr("sum(id)")
  WholeStageSum.explain()
  WholeStageSum.show()



  def main(args: Array[String]): Unit = {
    Thread.sleep(10000000)

  }

}
