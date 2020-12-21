# BigDataLabs

load script
chmod +x
#create dirs:
hdfs dfs -mkdir -p /bdpc/hive/data/

#create bucket, uploud data
copy from gs:
hdfs dfs -cp gs://data_fly/airlines.csv /bdpc/hive/data/airlines
hdfs dfs -cp gs://data_fly/flights.csv /bdpc/hive/data/flights
