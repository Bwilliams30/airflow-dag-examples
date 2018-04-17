from datetime import timedelta, datetime
import os

from airflow import DAG
from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator, \
    DataprocClusterDeleteOperator,DataProcHadoopOperator
from airflow.models import Variable

OUTPUT_FILE=os.path.join(Variable.get(
  'gcs_bucket'),"wordcount", datetime.now().strftime("%Y%m%d-%H%M%S")) + os.sep
WORDCOUNT_JAR='file:///usr/lib/hadoop-mapreduce/hadoop-mapreduce-examples.jar'
WORDCOUNT_ARGS=['wordcount', 'gs://pub/shakespeare/rose.txt', OUTPUT_FILE]

YESTERDAY = datetime.combine(datetime.today() - timedelta(1),
                             datetime.min.time())

DEFAULT_DAG_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': YESTERDAY,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'project_id': Variable.get('gcp_project')
}

with DAG('composer-quickstart-geh', schedule_interval=timedelta(days=1),
         default_args=DEFAULT_DAG_ARGS) as dag:

    # Create a Cloud Dataproc cluster.
    CREATE_DATAPROC_CLUSTER = DataprocClusterCreateOperator(
        task_id='create_dataproc_cluster',
        cluster_name='quickstart-cluster-{{ ds_nodash }}',
        num_workers=2,
        zone=Variable.get('gce_zone')
    )

    # Run the Hadoop wordcount example installed on the Cloud Dataproc
    # cluster master node.
    RUN_DATAPROC_HADOOP = DataProcHadoopOperator(
        task_id='run_dataproc_hadoop',
        main_jar=WORDCOUNT_JAR,
        cluster_name='quickstart-cluster-{{ ds_nodash }}',
        arguments=WORDCOUNT_ARGS
        )

    # Delete the Cloud Dataproc cluster.
    DELETE_DATAPROC_CLUSTER = DataprocClusterDeleteOperator(
        task_id='delete_dataproc_cluster',
        cluster_name='quickstart-cluster-{{ ds_nodash }}'
    )
    # Define DAG dependencies (sequencing).
    CREATE_DATAPROC_CLUSTER >> RUN_DATAPROC_HADOOP >> DELETE_DATAPROC_CLUSTER