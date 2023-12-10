package part2foundations

import org.apache.spark.sql.{DataFrame, SparkSession}

object CatalystDemo {

  val spark = SparkSession.builder()
    .appName("Catalyst Demo")
    .config("spark.sql.adaptive.enabled", "false")
    .master("local")
    .getOrCreate()

  import spark.implicits._

  val flights = spark
    .read
    .option("inferSchema", "true")
    .json("src/main/resources/data/flights/flights.json")

  val notFromHere = flights
    .filter($"origin" =!= "LGA")
    .filter($"origin" =!= "ORD")
    .filter($"origin" =!= "SFO")
    .filter($"origin" =!= "DEN")
    .filter($"origin" =!= "BOS")
    .filter($"origin" =!= "EWR")

  notFromHere.explain(true)

  def filterTeam1(flights: DataFrame) = flights.filter($"origin"=!= "LGA").filter($"dest"==="DEN")
  def filterTeam2(flights: DataFrame) = flights.filter($"origin"=!= "EWR").filter($"dest"==="DEN")

  val filtersBoth = filterTeam1(filterTeam2(flights))
  filtersBoth.explain(true)


  // FILTER PUSHDOWN OPTIMIZATION
  flights.write.save("src/main/resources/data/flights_parquet")
  val notFromLGA = spark.read.load("src/main/resources/data/flights_parquet")
    .filter($"origin"=!="LGA")

  notFromLGA.explain()

  def main(args: Array[String]): Unit = {

  }
}
