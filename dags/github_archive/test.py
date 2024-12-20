from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import datetime as dt
import os
import requests
import shutil
import pendulum

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

with DAG(dag_id="yajl_dag_new", start_date=pendulum.datetime(2024,11,0o7,tz="CET"), schedule_interval='@hourly', catchup=False) as dag:
 
       # january 
       getfiles_jan = PythonOperator(task_id="getfiles_jan", python_callable=my_func, op_args=['https://data.gharchive.org/2024-01-01-23.json.gz'])
       getfiles_jancheck = BashOperator(task_id="getfiles_jancheck",bash_command="zcat /tmp/2024-01-01-23.json.gz > /tmp/2024-01-01-23.json && sleep 10")
       create_table = SQLExecuteQueryOperator(
           task_id="create_table",
           autocommit=True,
           conn_id="postgres_conn",
           sql=f"CREATE TABLE temp (data jsonb);",
           split_statements=True,
           return_last=False,
       )
       create_data = SQLExecuteQueryOperator(
           task_id="create_data",
           autocommit=True,
           conn_id="postgres_conn",
           sql=f"\copy temp from '/tmp/2024-01-01-23.json'",
           split_statements=True,
           return_last=False,
       )
    
       # feb \copy t from /tmp/2024-01-01-23.json
       getfiles_feb = PythonOperator(task_id="getfiles_feb", python_callable=my_func, op_args=['https://data.gharchive.org/2024-02-01-23.json.gz'])

       # mar
       getfiles_mar = PythonOperator(task_id="getfiles_mar", python_callable=my_func, op_args=['https://data.gharchive.org/2024-03-01-23.json.gz'])
   

       
       getfiles_jan >> getfiles_jancheck >> create_table >> create_data >> getfiles_feb >> getfiles_mar 
