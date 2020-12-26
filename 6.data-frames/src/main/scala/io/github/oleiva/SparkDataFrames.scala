package io.github.oleiva

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object SparkDataFrames {

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("Wrong usage. Options: -in -out")
      sys.exit(2)
    }

    val in_bucket = args(0) // "gs://data_bucket/in/"
    val out_bucket = args(1) // "gs://data_bucket/out/"

    val in_airlines: String = in_bucket + "airlines.csv";
    val in_airports: String = in_bucket + "airports.csv";
    val in_flights: String = in_bucket + "flights.csv";


    val out_mostPopularAirportRes = out_bucket + "mostPopularAirport"
    val out_mostPopularAirportDebug = out_bucket + "mostPopularAirportResDebug"
    val out_canceledFlightsAll = out_bucket + "canceledFlightsAllDebug";
    val out_canceledFiltered = out_bucket + "canceledFiltered";
    val out_canceledNOTFiltered = out_bucket + "canceledNOTFiltered";

    println("in_airlines : " + in_bucket);
    println("in_airports : " + in_airports);
    println("in_flights  : " + in_flights);

    val ses = SparkSession
      .builder
      .appName("spark-data-frames")
      .master("yarn")
      .getOrCreate()

    //    val container = new Container()
    //    ses.sparkContext.register(container, "accumulator")
    import ses.implicits._

    val flightsDF: DataFrame = ses.read
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(in_flights)
      .select($"MONTH", $"AIRLINE".as("AIRLINE_FLIGHT"), $"DESTINATION_AIRPORT", $"ORIGIN_AIRPORT", $"CANCELLED")

    val airlinesDF: DataFrame = ses
      .read.option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(in_airlines)
      .select($"IATA_CODE".as("AIRLINES_IATA_CODE"), $"AIRLINE".as("AIRLINES_AIRLINE"))

    val airportsDF: DataFrame = ses.read
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(in_airports)
      .select($"IATA_CODE", $"AIRPORT")

    airlinesDF.printSchema();
    airportsDF.printSchema();
    flightsDF.printSchema();


    val allAirportsDf =
      flightsDF
        .groupBy($"MONTH", $"DESTINATION_AIRPORT").count().as("COUNT")
        .sort($"MONTH", $"COUNT".desc)
        .join(broadcast(airportsDF), $"DESTINATION_AIRPORT" === $"IATA_CODE")
        .select($"MONTH", $"DESTINATION_AIRPORT", $"AIRPORT".as("AIRPORT_NAME"), $"COUNT")

    allAirportsDf.show();

    val mostPopularAirport =
      allAirportsDf
        .withColumn("row", row_number.over(Window.partitionBy($"MONTH").orderBy($"COUNT".desc)))
        .where($"row" === 1).drop("row")
        .sort($"MONTH")
        .persist()

    mostPopularAirport.show();

    val canceledFlightsAll = flightsDF
      .groupBy("DESTINATION_AIRPORT", "AIRLINE_FLIGHT")
      .agg(
        sum("CANCELLED").as("SUM"),
        count("CANCELLED").as("COUNT"),
        ((sum("CANCELLED") / count("CANCELLED") * 100).as("PERCENTAGE")))
      .join(broadcast(airportsDF), $"DESTINATION_AIRPORT" === $"IATA_CODE")
      .join(broadcast(airlinesDF), $"AIRLINE_FLIGHT" === $"AIRLINES_IATA_CODE")
      .select(
        $"AIRLINES_AIRLINE".as("AIRLINE_NAME"),
        $"AIRPORT".as("ORIGIN_AIRPORT_NAME"),
        $"PERCENTAGE".as("PERCENTAGE"),
        $"SUM".as("NUMBER_OF_CANCELED_FLIGHTS"),
        $"COUNT".as("NUMBER_OF_PROCESSED_FLIGHTS"),
      )

    val filter = "Waco Regional Airport";
    val canceledFiltered = canceledFlightsAll
      .filter($"ORIGIN_AIRPORT_NAME" =!= filter)
      .sort($"AIRLINE_NAME", $"ORIGIN_AIRPORT_NAME".desc);

    val canceledNOTFiltered = canceledFlightsAll
      .filter($"ORIGIN_AIRPORT_NAME" === filter)
      .sort($"AIRLINE_NAME", $"ORIGIN_AIRPORT_NAME".desc)

    canceledFiltered.show(50);
    canceledNOTFiltered.show(50);


    // JOB 1
    printDf(allAirportsDf, out_mostPopularAirportDebug);
    printDf(mostPopularAirport, out_mostPopularAirportRes);

    // JOB 2
    printDf(canceledFlightsAll, out_canceledFlightsAll);
    printDf(canceledFiltered, out_canceledFiltered);
    printDf(canceledNOTFiltered, out_canceledNOTFiltered);


    ses.stop()
    ses.close()
  }

  private def printDf(df: DataFrame, nameOut: String): Unit = {
    df.write
      .option("header", "false")
      .option("delimiter", "\t")
      .csv(nameOut);
  }
}
