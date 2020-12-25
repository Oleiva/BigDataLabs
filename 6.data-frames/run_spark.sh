
usage() {
  echo -e "Wrong usage. Options: -in_flights -in_airports -output"
  exit 1
}


while getopts ":g:f:t" opt; do
    case "$opt" in
        g)  IN_FLIGHTS_PATH=${OPTARG} ;;
        f)  IN_AIRPORTS_PATH=${OPTARG} ;;
        t)  OUTPUT_FOLDER=${OPTARG} ;;
        *)  usage ;;
    esac
done


if [[ -z "$IN_FLIGHTS_PATH" ]];
then
  IN_FLIGHTS_PATH="gs://data_bucket/flights/flights.csv"
fi


if [[ -z "$IN_AIRPORTS_PATH" ]];
then
  IN_AIRPORTS_PATH="gs://data_bucket/flights/airports.csv"
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
	     spark-streaming-1.0.0-SNAPSHOT.jar "$IN_AIRPORTS_PATH" "$IN_AIRPORTS_PATH" "$OUTPUT_FOLDER"

echo "<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<"
