load script
chmod +x
#create dirs:
hdfs dfs -mkdir -p /bdpc/hive/data/

#create bucket, uploud data
copy from gs:
hdfs dfs -cp gs://data_fly/airlines.csv /bdpc/hive/data/airlines
hdfs dfs -cp gs://data_fly/flights.csv /bdpc/hive/data/flights
#airport


DROP DATABASE IF EXISTS HadoopHive CASCADE;
DROP DATABASE IF EXISTS FlyLines CASCADE;

CREATE DATABASE HadoopHive;
USE HadoopHive;


CREATE EXTERNAL TABLE IF NOT EXISTS FlyLines
(YEAR int, MONTH int, DAY int, DAY_OF_WEEK int, AIRLINE string,FLIGHT_NUMBER string, TAIL_NUMBER string, ORIGIN_AIRPORT string, DESTINATION_AIRPORT string, SCHEDULED_DEPARTURE string, DEPARTURE_TIME int,
 DEPARTURE_DELAY int,TAXI_OUT string, WHEELS_OFF string, SCHEDULED_TIME string, ELAPSED_TIME string, AIR_TIME string, DISTANCE string,
 WHEELS_ON string, TAXI_IN string, SCHEDULED_ARRIVAL string, ARRIVAL_TIME string, ARRIVAL_DELAY string, DIVERTED string, CANCELLED string,
 CANCELLATION_REASON string, AIR_SYSTEM_DELAY string, SECURITY_DELAY string, AIRLINE_DELAY string, LATE_AIRCRAFT_DELAY string, WEATHER_DELAY string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs:///bdpc/hive/flights';

CREATE EXTERNAL TABLE IF NOT EXISTS Airlines(AIRLINE string, AIRNAME string);

ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs:///bdpc/hive/airlines';


SELECT F.AIRLINE, A.AIRNAME, AVG(F.DEPARTURE_DELAY) as AvgDelay
FROM FlyLines F JOIN Airlines A ON F.AIRLINE = A.AIRLINE
GROUP BY F.AIRLINE, A.AIRNAME
ORDER BY AvgDelay DESC LIMIT 5;load script
chmod +x
#create dirs:
hdfs dfs -mkdir -p /bdpc/hive/data/

#create bucket, uploud data
copy from gs:
hdfs dfs -cp gs://data_fly/airlines.csv /bdpc/hive/data/airlines
hdfs dfs -cp gs://data_fly/flights.csv /bdpc/hive/data/flights
#airport


DROP DATABASE IF EXISTS HadoopHive CASCADE;
DROP DATABASE IF EXISTS FlyLines CASCADE;

CREATE DATABASE HadoopHive;
USE HadoopHive;


CREATE EXTERNAL TABLE IF NOT EXISTS FlyLines
(YEAR int, MONTH int, DAY int, DAY_OF_WEEK int, AIRLINE string,FLIGHT_NUMBER string, TAIL_NUMBER string, ORIGIN_AIRPORT string, DESTINATION_AIRPORT string, SCHEDULED_DEPARTURE string, DEPARTURE_TIME int,
 DEPARTURE_DELAY int,TAXI_OUT string, WHEELS_OFF string, SCHEDULED_TIME string, ELAPSED_TIME string, AIR_TIME string, DISTANCE string,
 WHEELS_ON string, TAXI_IN string, SCHEDULED_ARRIVAL string, ARRIVAL_TIME string, ARRIVAL_DELAY string, DIVERTED string, CANCELLED string,
 CANCELLATION_REASON string, AIR_SYSTEM_DELAY string, SECURITY_DELAY string, AIRLINE_DELAY string, LATE_AIRCRAFT_DELAY string, WEATHER_DELAY string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs:///bdpc/hive/flights';

CREATE EXTERNAL TABLE IF NOT EXISTS Airlines(AIRLINE string, AIRNAME string);

ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs:///bdpc/hive/airlines';


SELECT F.AIRLINE, A.AIRNAME, AVG(F.DEPARTURE_DELAY) as AvgDelay
FROM FlyLines F JOIN Airlines A ON F.AIRLINE = A.AIRLINE
GROUP BY F.AIRLINE, A.AIRNAME
ORDER BY AvgDelay DESC LIMIT 5;