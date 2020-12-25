package io.github.oleiva

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

object SparkDataFrames {

  def main(args: Array[String]): Unit = {
    if (args.length != 4) {
      println("Wrong usage. Options: -in_airlines -in_airports -in_flights -output")
      sys.exit(2)
    }


    val in_airlines: String = args(0);
    val in_airports = args(1);
    val in_flights = args(2);
    val outputBucket = args(3)
    val mostPopularAirportRes = outputBucket + "/mostPopularAirport"

    println("in_airlines : " + in_airlines);
    println("in_airports : " + in_airports);
    println("in_flights  : " + in_flights);
    println("output      : " + outputBucket)


    val ses = SparkSession
      .builder
      .appName("spark-data-frames")
      .master("yarn")
      .getOrCreate()


    val container = new Container()
    ses.sparkContext.register(container, "accumulator")
    import ses.implicits._

    val flightsDF: DataFrame = ses.read
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(in_flights)
      .select($"MONTH", $"AIRLINE", $"DESTINATION_AIRPORT", $"ORIGIN_AIRPORT", $"CANCELLED")

    val airlinesDF: DataFrame = ses
      .read.option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(in_airlines)

    val airportsDF: DataFrame = ses.read
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(in_airports)
      .select($"IATA_CODE", $"AIRPORT")

    airlinesDF.printSchema();
    airportsDF.printSchema();
    flightsDF.printSchema();


    val mostPopularAirport =
      flightsDF
        .groupBy($"MONTH", $"DESTINATION_AIRPORT").count().as("COUNT")
        .sort($"MONTH", $"COUNT".desc)
        .join(broadcast(airportsDF), $"DESTINATION_AIRPORT" === $"IATA_CODE")
        .select($"MONTH", $"DESTINATION_AIRPORT", $"AIRPORT".as("AIRPORT_NAME"), $"COUNT")
        .withColumn("row", row_number.over(Window.partitionBy($"MONTH").orderBy($"COUNT".desc)))
        .where($"row" === 1).drop("row")
        .sort($"MONTH")
        .persist()

    mostPopularAirport.show();


    // OUT
    mostPopularAirport.write.mode("append").json(mostPopularAirportRes)

    ses.stop()
    ses.close()
  }


  class Container extends AccumulatorV2[String, mutable.HashMap[String, Int]] {
    private val hashMap = new mutable.HashMap[String, Int]()

    override def isZero: Boolean = {
      hashMap.isEmpty
    }

    override def copy(): AccumulatorV2[String, mutable.HashMap[String, Int]] = {
      val newAcc = new Container()
      hashMap.synchronized {
        newAcc.hashMap ++= hashMap
      }
      newAcc
    }

    override def reset(): Unit = {
      hashMap.clear()
    }

    def add(airline: String) = {
      hashMap.get(airline) match {
        case Some(v) => hashMap += ((airline, v + 1))
        case None => hashMap += ((airline, 1))
      }
    }

    override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Int]]): Unit = {
      other match {
        case acc: AccumulatorV2[String, mutable.HashMap[String, Int]] => {
          for ((k, v) <- acc.value) {
            hashMap.get(k) match {
              case Some(newv) => hashMap += ((k, v + newv))
              case None => hashMap += ((k, v))
            }
          }
        }
      }
    }

    override def value: mutable.HashMap[String, Int] = hashMap

  }


}
