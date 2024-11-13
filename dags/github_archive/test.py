from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from random import randint
from datetime import datetime
import os
import requests
import shutil
import pendulum
import urllib.request
from airflow.decorators import task
import psycopg2

path = '/tmp'

def my_func(*op_args):
    #print(op_args)
    url=op_args[0]
    local_filename = url.split('/')[-1]
    try:
       filepath = os.path.join(path, local_filename)
       with requests.get(url, stream = True) as r:
         with open(filepath, 'wb') as f:
           shutil.copyfileobj(r.raw, f)
    except Exception as e:
        log.error(e)
        raise AirflowException(e)    

def execdb(*op_args):
    #sqlstatement=op_args[0]
    connection = psycopg2.connect(database="archive", user='pgbouncer', password='{{ var.value.pgpass }}', host="hippo-pgbouncer.etl-db.svc", port=5432)
    try:
      cursor = connection.cursor()
      sql_context ="""
      \l;
      """
      cursor.execute(sql_context)
    except psycopg2.OperationalError:
      pass
    
    # Fetch all rows from database
    # record = cursor.fetchall()
    # print("Data from Database:- ", record)


with DAG(dag_id="yajl_dag_new", start_date=pendulum.datetime(2024,11,0o7,tz="CET"), schedule_interval='@hourly', catchup=False) as dag:
 
       # january 
       getfiles_jan = PythonOperator(task_id="getfiles_jan", python_callable=my_func, op_args=['https://data.gharchive.org/2024-01-01-23.json.gz'])
       getfiles_jancheck = BashOperator(task_id="getfiles_jancheck",bash_command="[[ -f /tmp/2024-01-01-23.json.gz ]] && echo 'File found!' && ls -ltr /tmp/2024-01-01-23.json.gz && gunzip -c /tmp/2024-01-01-23.json.gz >/tmp/2024-01-01-23.json")

       # feb
       getfiles_feb = PythonOperator(task_id="getfiles_feb", python_callable=my_func, op_args=['https://data.gharchive.org/2024-02-01-23.json.gz'])

       # mar
       getfiles_mar = PythonOperator(task_id="getfiles_mar", python_callable=my_func, op_args=['https://data.gharchive.org/2024-03-01-23.json.gz'])

       # db
       execdb = PythonOperator(task_id="execdb", python_callable=execdb, op_args=['\c archive'])

    
       getfiles_jan >> getfiles_jancheck >> getfiles_feb >> getfiles_mar >> execdb
       #importall_feb >> importall_jan >> importall_mar 
