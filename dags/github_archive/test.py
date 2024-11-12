from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from random import randint
from datetime import datetime
import pendulum


def check_process(file_name):
    import os
    import sys
    #import io
    #import requests
    #import gzip
    #import json

    BASEPATH = os.path.dirname(os.path.realpath(__file__))
    sys.path = [BASEPATH, '%s/..' %BASEPATH] + sys.path
    from yajl import YajlContentHandler, YajlParser

    # Sample callbacks, which output some debug info
    # these are examples to show off the yajl parser
    class ContentHandler(YajlContentHandler):
        def __init__(self):
            self.out = sys.stdout
        def yajl_null(self, ctx):
            self.out.write("null\n" )
        def yajl_boolean(self, ctx, boolVal):
            self.out.write("bool: %s\n" %('true' if boolVal else 'false'))
        def yajl_integer(self, ctx, integerVal):
            self.out.write("integer: %s\n" %integerVal)
        def yajl_double(self, ctx, doubleVal):
            self.out.write("double: %s\n" %doubleVal)
        def yajl_number(self, ctx, stringNum):
            ''' Since this is defined both integer and double callbacks are useless '''
            num = float(stringNum) if '.' in stringNum else int(stringNum)
            self.out.write("number: %s\n" %num)
        def yajl_string(self, ctx, stringVal):
            self.out.write("string: '%s'\n" %stringVal)
        def yajl_start_map(self, ctx):
            self.out.write("map open '{'\n")
        def yajl_map_key(self, ctx, stringVal):
            self.out.write("key: '%s'\n" %stringVal)
        def yajl_end_map(self, ctx):
            self.out.write("map close '}'\n")
        def yajl_start_array(self, ctx):
            self.out.write("array open '['\n")
        def yajl_end_array(self, ctx):
            self.out.write("array close ']'\n")


    def main(args):
        parser = YajlParser(ContentHandler())
        parser.allow_multiple_values = True
        if args:
            for fn in args:
                f = open(fn)
                parser.parse(f=f)
                f.close()
        else:
            parser.parse()
        return 0

    if __name__ == "__main__":
        raise SystemExit(main(sys.argv[1:]))


with DAG(dag_id="yajl_dag_new", start_date=pendulum.datetime(2024,11,0o7,tz="CET"), schedule_interval='@hourly', catchup=False) as dag:

       # install python packages and build them
       #  && ./configure && make && sudo make install
       pypacks = BashOperator(
           task_id="install_yajl",
           bash_command="git clone https://github.com/lloyd/yajl.git && cd yajl"
       )
    
       # january
       getfiles_jan = BashOperator(task_id="getfiles_jan",bash_command="curl -L https://data.gharchive.org/2024-01-01-23.json.gz | gzip -d > 2024-1.json")
       importall_jan = PythonOperator(task_id="jan_process",python_callable=check_process,op_kwargs={"file_name": "2024-1.json"})

       # feb
       getfiles_feb = BashOperator(task_id="getfiles_feb",bash_command="curl -L https://data.gharchive.org/2024-02-01-23.json.gz | gzip -d > 2024-2.json")
       importall_feb = PythonOperator(task_id="feb_process",python_callable=check_process,op_kwargs={"file_name": "2024-2.json"})

       # mar
       getfiles_mar = BashOperator(task_id="getfiles_mar",bash_command="curl -L https://data.gharchive.org/2024-03-01-23.json.gz | gzip -d > 2024-3.json")
       importall_mar = PythonOperator(task_id="mar_process",python_callable=check_process,op_kwargs={"file_name": "2024-3.json"})
       
       #for i in range(1, 4):
       #    importall = PythonOperator(task_id=f"{i}_process",python_callable=check_process,op_kwargs={"file_name": f"2024-{i}.json"})

       pypacks >> getfiles_jan >> importall_jan >> getfiles_feb >> importall_feb >> getfiles_mar >> importall_mar 
