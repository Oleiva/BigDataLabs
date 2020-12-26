
usage() {
  echo -e "Wrong usage. Options: -in -out"
  exit 1
}


while getopts ":g:f:t" opt; do
    case "$opt" in
        g)  IN_PATH=${OPTARG} ;;
        f)  OUTPUT_FOLDER=${OPTARG} ;;
        *)  usage ;;
    esac
done



if [[ -z "$IN_PATH" ]];
then
  IN_PATH="gs://data_bucket/in/"
fi


if [[ -z "$OUTPUT_FOLDER" ]];
then
  OUTPUT_FOLDER="gs://data_bucket/out/"
fi



spark-submit --master yarn \
             --num-executors 20 --executor-memory 1G --executor-cores 1 --driver-memory 1G \
             --conf spark.ui.showConsoleProgress=true \
             --class io.github.oleiva.SparkDataFrames \
             --packages org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.7 \
	           spark-data-frames-1.0.0-SNAPSHOT.jar "$IN_PATH" "$OUTPUT_FOLDER"

echo "<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<"
