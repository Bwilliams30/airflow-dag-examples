from datetime import timedelta, datetime

from airflow import DAG
from airflow.models import Variable
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
import random
import string

BQL_ARG = Variable.get('gcp_bq_insert_query')

YESTERDAY = datetime.combine(datetime.today() - timedelta(1),
                             datetime.min.time())

DEFAULT_DAG_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': YESTERDAY,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Create Random Values to Insert on BigQuery
qtr = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10))
sales = ''.join(random.choice(string.digits) for _ in range(5))
year = '"2018"'
bql = "INSERT `" + BQL_ARG + "`(qtr,sales,year,datetime) VALUES(\"" + qtr + "\"""," + sales + "," + year + ", CURRENT_DATETIME())"
print ("Inserting: " + bql)

dag = DAG('composer-geh-bq', schedule_interval=timedelta(days=1), default_args=DEFAULT_DAG_ARGS)
# Schedule Simple Insert on Existing BigQuery Table
INSERT_SQL_TABLE = BigQueryOperator(
    task_id='insertQueryData',
    bql=bql,
    use_legacy_sql=False,
    dag=dag
)
