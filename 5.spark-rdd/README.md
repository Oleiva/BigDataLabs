# BigDataLabs

## Spark-RDD

 1. Copy data from the bucket
  
  `hdfs dfs -cp gs://data_flys/flights.csv /bdpc/data/flights.csv
  `hdfs dfs -cp gs://data_flys/airports.csv /bdpc/data/airports.csv`
  
   2. Create dir
   `hdfs dfs -mkdir -p /bdpc/data/top/`
   
  3. chmod +x run_spark.sh
  4. run Spark `./run_spark.sh`