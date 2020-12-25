package io.github.oleiva

import org.apache.spark.sql.functions.{broadcast, count, round, when}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.row_number
import scala.collection.mutable
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.rank
import org.apache.spark.sql.expressions.Window

object SparkDataFramesLocal {

  def main(args: Array[String]): Unit = {

    val in_airlines = "/Users/ivasoft/Development/GL_Work/data/airlines.csv";
    val in_flights = "/Users/ivasoft/Development/GL_Work/data/flights_mini.csv";
    val in_airports = "/Users/ivasoft/Development/GL_Work/data/airports.csv";

    val mostPopularAirportRes = "/Users/ivasoft/Development/GL_Work/data/mostPopularAirportRes.csv";


    val ses = SparkSession.builder.
      master("local")
      .appName("spark session example")
      .getOrCreate();

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

    mostPopularAirport.write.mode("append").json(mostPopularAirportRes)


  }


}
