from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from random import randint
from datetime import datetime
import pendulum


with DAG(dag_id="yajl_dag_new", start_date=pendulum.datetime(2024,11,0o7,tz="CET"), schedule_interval='@hourly', catchup=False) as dag:
 
       # january | gzip -d > 2024-1.json
       getfiles_jan = BashOperator(task_id="getfiles_jan",bash_command="curl -L https://data.gharchive.org/2024-01-01-23.json.gz")
       # importall_jan = PythonOperator(task_id="jan_process",python_callable=check_process,op_kwargs={"file_name": "2024-1.json"})

       # feb | gzip -d > 2024-2.json
       getfiles_feb = BashOperator(task_id="getfiles_feb",bash_command="curl -L https://data.gharchive.org/2024-02-01-23.json.gz")
       # importall_feb = PythonOperator(task_id="feb_process",python_callable=check_process,op_kwargs={"file_name": "2024-2.json"})

       # mar | gzip -d > 2024-3.json
       getfiles_mar = BashOperator(task_id="getfiles_mar",bash_command="curl -L https://data.gharchive.org/2024-03-01-23.json.gz")
       # importall_mar = PythonOperator(task_id="mar_process",python_callable=check_process,op_kwargs={"file_name": "2024-3.json"})
       
       #for i in range(1, 4):
       #    importall = PythonOperator(task_id=f"{i}_process",python_callable=check_process,op_kwargs={"file_name": f"2024-{i}.json"})

       getfiles_jan >> getfiles_feb >> getfiles_mar
       #importall_feb >> importall_jan >> importall_mar 
