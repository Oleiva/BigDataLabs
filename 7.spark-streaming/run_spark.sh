
usage() {
  echo -e "Wrong usage. Options: -kafkaTopic -gcBucketPath -checkpointLocation"
  exit 1
}


while getopts ":g:f:t" opt; do
    case "$opt" in
        g)  BUCKET=${OPTARG} ;;
        f)  OUTPUT_FOLDER=${OPTARG} ;;
        t)  KAFKA_TOPIC=${OPTARG} ;;
        *)  usage ;;
    esac
done

if [[ -z "$KAFKA_TOPIC" ]];
then
  KAFKA_TOPIC="bitcoin-transactions"
fi

if [[ -z "$BUCKET" ]];
then
  BUCKET="spark-stream-test"
fi

if [[ -z "$OUTPUT_FOLDER" ]];
then
  OUTPUT_FOLDER="out"
fi

if [[ -z "$CHECKPOINT_LOCATION" ]];
then
  CHECKPOINT_LOCATION="/bdpc/data/spark-out"
fi



spark-submit --master yarn \
             --num-executors 20 --executor-memory 1G --executor-cores 1 --driver-memory 1G \
             --conf spark.ui.showConsoleProgress=true \
             --class io.github.oleiva.SparkStreaming \
             --packages org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.7 \
	     spark-streaming-1.0.0-SNAPSHOT.jar "$KAFKA_TOPIC" "gs://$BUCKET/$OUTPUT_FOLDER" "$CHECKPOINT_LOCATION"

echo "<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<"
