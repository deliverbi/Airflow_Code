#===========================================================================
#Program Name           :        Check_Previous_Run.py
#Date                   :        02-Nov-2019
#Version                :        1.0
#Author                 :        Krishna Udathu (KM) and Shahed Munir (SM)
#Description            :        Checks for the previous run failure of itself and fails on the first task of the current run
#Revision History
#===========================================================================
#Author              Date            Version Change Description
#===========================================================================
#KM and SM           02-Nov-2019     1.0     Gave Life
#
#
#===========================================================================
from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.hooks.sftp_hook import SFTPHook
from airflow.hooks.mysql_hook import MySqlHook
import codecs
import os
import logging
from airflow.operators.email_operator import EmailOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable
from airflow import AirflowException
from airflow.hooks.jdbc_hook import JdbcHook
from airflow.utils import timezone

DAG_OWNER_NAME = "airflow"
ALERT_EMAIL_ADDRESSES = ["krishna.udathu@deliverbi.com"]

default_args = {
    'owner': DAG_OWNER_NAME,
    'email': ALERT_EMAIL_ADDRESSES,
    'email_on_failure': True,
    'email_on_retry': False,
    'start_date': datetime(2019, 2, 5, 12, 10),
    'retries': 0,
    #'sla': timedelta(seconds=30),
    'retry_delay': timedelta(minutes=1)
    }

# This dag is scheduled to run daily 3 times at 10.35, 10.40 and 10.45

dag = DAG('Check_Previous_Run', default_args=default_args, schedule_interval='35,40,45 10 * * *',max_active_runs=1,catchup=False)

def check_previous_runs(**kwargs):
    context = kwargs
    current_run_id = context['dag_run'].run_id
    current_dag_id = context['dag_run'].dag_id
    # Connect to mysql and check for any errors for this DAG
    airflow_conn = MySqlHook(mysql_conn_id='deliverbi_mysql_airflow')
    l_error_count = 0
    cmd_sql = f"select count(1) from airflow.dag_run where dag_id = '{current_dag_id}' "
    cmd_sql += f"and run_id <> '{current_run_id}' and state = 'failed'"
    print(cmd_sql)
    airflow_data = airflow_conn.get_records(sql=cmd_sql)
    for row in airflow_data:
      l_error_count = int((str(row[0])))

    print("Found Previous Errors:" + str(l_error_count))
    if l_error_count != 0:
      raise AirflowException("Previous Run in Error so Failing the Current Run")

# Tasks
check_previous_run_status = PythonOperator(task_id='check_previous_run_status',provide_context=True,python_callable=check_previous_runs,dag=dag)

task1 = DummyOperator(task_id='task1', retries=2, dag=dag)

check_previous_run_status.set_downstream(task1)


