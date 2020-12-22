# BigDataLabs

## Spark-RDD

1. Upload jar and sh script
2. chmod +x run_spark.sh

3. # Create a topic
`bin/kafka-topics.sh --create \
  --zookeeper localhost:2181 \
  --replication-factor 1 --partitions 13 \
  --topic bitcoin-transactions`

4. Prepare and run Nifi using Lab_7_spark_streaming.xml
and default settings from Lab2

5  Create gc bucket

6. run Spark `./run_spark.sh`
