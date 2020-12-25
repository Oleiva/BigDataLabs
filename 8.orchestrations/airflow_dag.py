import datetime
import os

from airflow import models
from airflow.contrib.operators import dataproc_operator
from airflow.utils import trigger_rule
from airflow.contrib.sensors.gcs_sensor import GoogleCloudStorageObjectSensor

# Output file for Cloud Dataproc job.

var_bucket_name = models.Variable.get('gcs_bucket')
var_project_id = models.Variable.get('gcp_project')
var_zone = models.Variable.get('gce_zone')

dateTimePattern = datetime.datetime.now().strftime('%Y%m%d-%H%M%S')
naming_cluster = 'composer-spark-cluster'

output_file = os.path.join(
    var_bucket_name, 'spark_calculation',
    dateTimePattern) + os.sep
# Path to Hadoop wordcount example available on every Dataproc cluster.
SPARK_JAR = (
    'file:///usr/lib/spark-TSV-1.0.jar'
)

flights_file = var_bucket_name + '/flights/' + 'flights.csv'
airports_file = var_bucket_name + '/airports/' + 'airports.csv'

executor_args = [flights_file, airports_file, output_file]


default_dag_args = {
    # Setting start date as yesterday starts the DAG immediately when it is
    # detected in the Cloud Storage bucket.
    'start_date': datetime.now(),
    # To email on failure or retry set 'email' arg to your email and enable
    # emailing here.
    'email_on_failure': False,
    'email_on_retry': False,
    # If a task fails, retry it once after waiting at least hours=2
    'retries': 2,
    'retry_delay': datetime.timedelta(minutes=7),
    'project_id': var_project_id
}

# [START composer_hadoop_schedule]
with models.DAG(
        'airflow',
        # Continue to run DAG once per day
        schedule_interval=datetime.timedelta(hours=1),
        default_args=default_dag_args,
        catchup=False) as dag:
    # [END composer_hadoop_schedule]

    # Sensor
    gcs_file_sensor = GoogleCloudStorageObjectSensor(
        task_id='waiting_file_sensor',
        timeout=120,
        bucket=var_bucket_name,
        soft_fail=True,
        object='flights/{{ execution_date.format("%Y/%m/%d/%H") }}/_SUCCESS')

    # Create a Cloud Dataproc cluster.
    create_dataproc_cluster = dataproc_operator.DataprocClusterCreateOperator(
        task_id='create_dataproc_cluster',
        # Give the cluster a unique name by appending the date scheduled.
        # See https://airflow.apache.org/code.html#default-variables
        cluster_name=naming_cluster + '-{{ ds_nodash }}',
        num_workers=2,
        zone=var_zone,
        master_machine_type='n1-standard-1',
        worker_machine_type='n1-standard-1')

    # Run the Hadoop wordcount example installed on the Cloud Dataproc cluster
    # master node.
    run_dataproc_spark = dataproc_operator.DataProcHadoopOperator(
        task_id='run_dataproc_hadoop',
        main_jar=SPARK_JAR,
        cluster_name=naming_cluster + '-{{ ds_nodash }}',
        arguments=executor_args)

    # Delete Cloud Dataproc cluster.
    delete_dataproc_cluster = dataproc_operator.DataprocClusterDeleteOperator(
        task_id='delete_dataproc_cluster',
        cluster_name=naming_cluster + '-{{ ds_nodash }}',
        # Setting trigger_rule to ALL_DONE causes the cluster to be deleted
        # even if the Dataproc job fails.
        trigger_rule=trigger_rule.TriggerRule.ALL_DONE)

    # [START composer_hadoop_steps]
    # Define DAG dependencies.
    create_dataproc_cluster >> run_dataproc_spark >> delete_dataproc_cluster
    # [END composer_hadoop_steps]

# [END composer_hadoop_tutorial]
