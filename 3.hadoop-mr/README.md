# Hadoop MapReduce Word Counter Application example

To upload samle data:
 - run `./upload_from_local_to_hdfs.sh -f txt` to upload `../data/word_count` to `/bdpc/hadoop_mr/word_count/input`
 - run `./upload_from_local_to_hdfs.sh -f gz` to upload `../data/word_count_gzip` to `/bdpc/hadoop_mr/word_count/input`

 
 /bdpc/hadoop_mr/word_count/input

<!-- load jar, script -->
chmod +x
<!-- create dir -->
hdfs dfs -mkdir -p /bdpc/hadoop_mr/word_count/input
hdfs dfs -mkdir -p /bdpc/hadoop_mr/word_count/output
create bucket, uploud data
<!-- copy from gs: -->
hdfs dfs -cp gs:<//data_fly/flights.csv> /bdpc/hadoop_mr/word_count/input
<!-- airport -->



To run:
 ./submit_word_counter.sh 

 To copy to local

hadoop fs -get /bdpc/hadoop_mr/word_count/output /sudo hadoop fs -get /bdpc/hadoop_mr/word_count/output/_SUCCESS /home/test.csvhome/hadoop_tp/