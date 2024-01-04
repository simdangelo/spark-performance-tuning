package part4partitioning

import org.apache.spark.{HashPartitioner, Partitioner, RangePartitioner}
import org.apache.spark.sql.SparkSession

import scala.util.Random

object Partitioners {

  val spark = SparkSession.builder()
    .appName("Partitioners")
    .config("spark.sql.adaptive.enabled", "false")
    .master("local")
    .getOrCreate()

  val sc = spark.sparkContext

//  val numbers = sc.parallelize(1 to 10000)
//  println(numbers.partitioner) // None
//
//  val numbers3 = numbers.repartition(3) // random data redistribution
//  println(numbers3.partitioner) // None
//
//  // keep track of the partitioner
//  // KV RDDs can control the partitioner scheme
//  val keyedNumbers = numbers.map(n => (n % 10, n)) // RDD[(Int, Int)]
//
//  val hashedNumbers = keyedNumbers.partitionBy(new HashPartitioner(4)) // keys with the same Hash will stay on the same partition
//  val rangedNumbers = keyedNumbers.partitionBy(new RangePartitioner(5, keyedNumbers)) // keys with the same range will be on the same partitio


  // define your own partitioner
  def generateRandomWords(nWords: Int, maxLenght: Int) = {
    val r = new Random()
    (1 to nWords).map(_ => r.alphanumeric.take(r.nextInt(maxLenght)).mkString(""))
  }

  val randomWordsRDD = sc.parallelize(generateRandomWords(1000, 100))
  // repartition this RDD by the words length == two words of the same length will be on the same partition
  // custom computation = counting the occurrences of "z" in every words
  val zWordsRDD = randomWordsRDD.map(word => (word, word.count(_ == "z")))// RDD[(String, Int)]

  class ByLengthPartitioner(override val numPartitions: Int) extends Partitioner {
    override def getPartition(key: Any): Int = {
      key.toString.length % numPartitions
    }
  }

  val byLengthZWords = zWordsRDD.partitionBy(new ByLengthPartitioner(100))

  def main(args: Array[String]): Unit = {
    byLengthZWords.foreachPartition(_.foreach(println))
  }
}
