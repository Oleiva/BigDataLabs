package io.github.oleiva

import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object SparkStreaming {
  val BASE_ON_EVENT_TIME = "1 minute";
  val ALLOWED_LATENCY = "3 minutes"
  val PROCESS_TIME = "60 seconds";

  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      println("Wrong usage. Options: -kafkaTopic -gcBucketPath -checkpointLocation")
      sys.exit(2)
    }

    val topic: String = args(0);
    val gcBucketPath = args(1);
    val checkpointLocation: String = args(2)

    println("topic "+topic);
    println("gcBucketPath "+gcBucketPath);
    println("checkpointLocation "+checkpointLocation)

    val ses = SparkSession
      .builder
      .appName("spark-streaming")
      .master("yarn")
      .getOrCreate()

    val df = ses
      .readStream.format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", topic)
      .load()

    job(df)
      .writeStream
      .format("json")
      .trigger(Trigger.ProcessingTime(PROCESS_TIME))
      .option("checkpointLocation", checkpointLocation)
      .start(gcBucketPath)
      .awaitTermination();

    ses.close()
  }


  /*
  [
    {
      "data": {
        "id": 1305493492117505,
        "id_str": "1305493492117505",
        "order_type": 1,
        "datetime": "1607558972",
        "microtimestamp": "1607558971757000",
        "amount": 0.3837784,
        "amount_str": "0.38377840",
        "price": 18494.72,
        "price_str": "18494.72"
      },
      "channel": "live_orders_btcusd",
      "event": "order_created"
    },
   */
  def job(df: DataFrame): DataFrame = {
    import df.sparkSession.implicits._

    val schema = new StructType().
      add("data", new StructType()
        .add("datetime", StringType)
        .add("amount", DoubleType)
        .add("price", DoubleType))

    df.select(from_json($"value".cast(StringType), schema).as("record"))
      .withColumn("timestamp", $"record.data.datetime".cast(LongType).cast(TimestampType))
      .withColumn("sales", $"record.data.amount" * $"record.data.price")
      .withWatermark("timestamp", ALLOWED_LATENCY)
      .groupBy(window($"timestamp", BASE_ON_EVENT_TIME))
      .agg(
        count("record.data.price").as("COUNT"),
        avg("record.data.price").as("AVERAGE_PRICE"),
        sum("sales").as("SALES_TOTAL")
      )
  }

}
