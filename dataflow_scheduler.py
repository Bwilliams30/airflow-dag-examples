from datetime import timedelta, datetime

from airflow import DAG
from airflow.contrib.operators.dataflow_operator import DataflowTemplateOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable

PROJECT = Variable.get('project')
TEMP_BUCKET = Variable.get('bucket')
TEMPLATE = 'gs://dataflow-templates/wordcount/template_file'
PARAMETERS = {
    'inputFile': 'gs://dataflow-samples/shakespeare/kinglear.txt',
    'output': TEMP_BUCKET + '/output/my_output'
}

TODAY = datetime.today()
TODAY_STRING = datetime.today().strftime('%Y%m%d')

DEFAULT_DAG_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': TODAY,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': '@hourly',
    'dataflow_default_options': {
               'project': PROJECT,
               'zone': 'us-east1-b',
               'stagingLocation': TEMP_BUCKET
           }
}

dag = DAG(
    'Demo-DAG-DataflowCronHourly',
    default_args=DEFAULT_DAG_ARGS,
    dagrun_timeout=timedelta(hours=1),
    schedule_interval='00 * * * *'
)

start = DummyOperator(task_id='inicio', dag=dag)
end = DummyOperator(task_id='fim', dag=dag)

t1 = DataflowTemplateOperator(
    task_id='dataflow_count_words_example',
    template=TEMPLATE,
    parameters=PARAMETERS,
    dag=dag)

start >> t1 >> end
