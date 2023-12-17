package part3caching

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object Checkpointing {

  val spark = SparkSession.builder()
    .appName("Checkpointing")
    .config("spark.sql.adaptive.enabled", "false")
    .master("local")
    .getOrCreate()

  val sc = spark.sparkContext

  def demoCheckpoint() = {
    val flightsDF = spark.read
      .option("inferSchema", "true")
      .json("src/main/resources/data/flights")

    // do some expensive computation
    val orderedFlights = flightsDF.orderBy("dist")
    orderedFlights.show()

    // checkpointing is used to avoid failure in computations
    // needs to be configured
    sc.setCheckpointDir("spark-warehouse")

    // checkpoint a DF  = save the DF to disk
    val checkpointedFlights = orderedFlights.checkpoint() // action

    // query plan difference with checkpointed DFs
    orderedFlights.explain()
    checkpointedFlights.explain()

    checkpointedFlights.show()
  }


  // caching vs checkpointing performance

//  RDDs case
  def cachingJobRDD() = {
    val numbers = sc.parallelize(1 to 10000000)
    val descNumbers = numbers.sortBy(-_).persist(StorageLevel.DISK_ONLY)
    descNumbers.sum()
    descNumbers.sum() // shorter time here
  }

  def checkpointingJobRDD() = {
    sc.setCheckpointDir("spark-warehouse")
    val numbers = sc.parallelize(1 to 10000000)
    val descNumbers = numbers.sortBy(-_)
    descNumbers.checkpoint() // returns Unit
    descNumbers.sum()
    descNumbers.sum()
    }

//  DFs case
  def cachingJobDF() = {
    val flightsDF = spark.read
      .option("inferSchema", "true")
      .json("src/main/resources/data/flights")

    val orderedFlights = flightsDF.orderBy("dist")
    orderedFlights.persist(StorageLevel.DISK_ONLY)
    orderedFlights.count()
    orderedFlights.count() // much shorted job
  }

  def checkpointingJobDF() = {
    sc.setCheckpointDir("spark-warehouse")

    val flightsDF = spark.read
      .option("inferSchema", "true")
      .json("src/main/resources/data/flights")

    val orderedFlights = flightsDF.orderBy("dist")
    val checkpointedFlights = orderedFlights.checkpoint()
    checkpointedFlights.count()
    checkpointedFlights.count()
  }

  def main(args: Array[String]): Unit = {
    cachingJobDF()
    checkpointingJobDF()

    Thread.sleep(1000000)
  }
}
