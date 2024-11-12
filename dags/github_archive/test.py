from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from random import randint
from datetime import datetime
import pendulum
import urllib.request
from airflow.decorators import task

path = '/tmp/folder'

if os.path.isdir(path):
    os.chdir(path)
else:
    os.mkdir(path)
    os.chdir(path)

#def download_file(uri, target_path):
def download_file(*op_args):
    with urllib.request.urlopen(op_args[0]) as file:
        with open(op_args[1], "wt") as new_file:
           new_file.write(file.read())

def my_func(*op_args):
    #print(op_args)
    filename=op_args[0]
    targetpath=op_args[1]
    try:
      with urllib.request.urlopen(filename) as file:
          with open(targetpath, "wt") as new_file:
              new_file.write(file.read())
        # return op_args[0]
    except Exception as e:
        log.error(e)
        raise AirflowException(e)

with DAG(dag_id="yajl_dag_new", start_date=pendulum.datetime(2024,11,0o7,tz="CET"), schedule_interval='@hourly', catchup=False) as dag:
 
       # january | gzip -d > 2024-1.json
       #
       getfiles_jan = PythonOperator(task_id="getfiles_jan", python_callable=my_func, op_args=['https://data.gharchive.org/2024-01-01-23.json.gz','/tmp'])
       getfiles_jancheck = BashOperator(task_id="getfiles_jancheck",bash_command="[[ -f /tmp/2024-01-01-23.json.gz ]] && echo 'File found!'")
       # importall_jan = PythonOperator(task_id="jan_process",python_callable=check_process,op_kwargs={"file_name": "2024-1.json"})

       # feb | gzip -d > 2024-2.json
       #getfiles_feb = BashOperator(task_id="getfiles_feb",bash_command="pip install wget && wget https://data.gharchive.org/2024-02-01-23.json.gz")
       # importall_feb = PythonOperator(task_id="feb_process",python_callable=check_process,op_kwargs={"file_name": "2024-2.json"})

       # mar | gzip -d > 2024-3.json
       #getfiles_mar = BashOperator(task_id="getfiles_mar",bash_command="pip install wget && wget https://data.gharchive.org/2024-03-01-23.json.gz")
       # importall_mar = PythonOperator(task_id="mar_process",python_callable=check_process,op_kwargs={"file_name": "2024-3.json"})
       
       #for i in range(1, 4):
       #    importall = PythonOperator(task_id=f"{i}_process",python_callable=check_process,op_kwargs={"file_name": f"2024-{i}.json"})

       getfiles_jan >> getfiles_jancheck 
       # >> getfiles_feb >> getfiles_mar
       #importall_feb >> importall_jan >> importall_mar 
