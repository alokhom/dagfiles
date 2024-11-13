from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.dates import days_ago
from random import randint
import datetime as dt
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

      results = cursor.execute(sql_context)
      cursor.close()
      connection.close()
      return results
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

       getfiles_jan >> getfiles_jancheck >> getfiles_feb >> getfiles_mar
       #importall_feb >> importall_jan >> importall_mar 

# db
#execdb = PythonOperator(task_id="execdb", python_callable=execdb, op_args=['\c archive'])
with DAG(dag_id="postgresop_demo", schedule="@once", start_date=dt.datetime(2021, 12, 01), catchup=False) as dag:
       create_pet_table = SQLExecuteQueryOperator(
           task_id="create_pet_table",
           conn_id="postgres_conn",
           sql="""
               CREATE TABLE IF NOT EXISTS pet (
               pet_id SERIAL PRIMARY KEY,
               name VARCHAR NOT NULL,
               pet_type VARCHAR NOT NULL,
               birth_date DATE NOT NULL,
               OWNER VARCHAR NOT NULL);
             """,
       )
       populate_pet_table = SQLExecuteQueryOperator(
           task_id="populate_pet_table",
           conn_id="postgres_conn",
           sql="""
                INSERT INTO pet (name, pet_type, birth_date, OWNER)
                VALUES ( 'Max', 'Dog', '2018-07-05', 'Jane');
                INSERT INTO pet (name, pet_type, birth_date, OWNER)
                VALUES ( 'Susie', 'Cat', '2019-05-01', 'Phil');
                INSERT INTO pet (name, pet_type, birth_date, OWNER)
                VALUES ( 'Lester', 'Hamster', '2020-06-23', 'Lily');
                INSERT INTO pet (name, pet_type, birth_date, OWNER)
                VALUES ( 'Quincy', 'Parrot', '2013-08-11', 'Anne');
               """,
       )
       get_all_pets = SQLExecuteQueryOperator(task_id="get_all_pets", sql="SELECT * FROM pet;")
       get_birth_date = SQLExecuteQueryOperator(
           task_id="get_birth_date",
           conn_id="postgres_conn",
           sql="SELECT * FROM pet WHERE birth_date BETWEEN SYMMETRIC %(begin_date)s AND %(end_date)s",
           parameters={"begin_date": "2020-01-01", "end_date": "2020-12-31"},
           hook_params={"options": "-c statement_timeout=3000ms"},
        )
       
       create_pet_table >> populate_pet_table >> get_all_pets >> get_birth_date

