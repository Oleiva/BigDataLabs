# BigDataLabs

## Orchestration with Apache Airflow

1. get jar-file from lab 6 and save to  'file:///usr/lib/spark-data-frames-1.0.0-SNAPSHOT.jar'
2. create composer cluster
3. create buckets and uploud files:
  <var_bucket_name> /in/airlines.csv'
  <var_bucket_name> /in/airports.csv'
  <var_bucket_name> /in/airports.csv'
4. add Airflow Variables using Airflow UI
 -gcs_bucket (spark_bucket)
 -gcp_project (spark_ex)
 -gce_zone (us-east1)

 5.upload DAG file into composer cluster (step 2)
