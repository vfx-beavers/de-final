
import pendulum
import os
import vertica_python
import logging
from airflow import DAG
from pathlib import Path
from airflow.models.variable import Variable
from airflow.operators.python import PythonOperator


# Airflow Vertica connection vars
vertica_conn_info = {
     'host': Variable.get('VERTICA_HOST'),
     'port': Variable.get('VERTICA_PORT'),
     'user': Variable.get('VERTICA_USER'),
     'password': Variable.get('VERTICA_PASSWORD'),
     'database': '',
     'autocommit': True
}

# External DDL files
sql_files = [
"/lessons/sql/ddl_currencies.sql",
"/lessons/sql/ddl_transactions.sql",
"/lessons/sql/ddl_global_metrics.sql"
]


def make_tables(sql_file):
     try:
           query = open(sql_file).read()
           with vertica_python.connect(**vertica_conn_info) as connection:
                cur_vertica = connection.cursor()
                cur_vertica.execute(query)
                cur_vertica.connection.commit()
                cur_vertica.close() 
     except Exception as e:
           logging.error(f"PG to vertica error in: {Path(sql_file).stem.replace('make_tables_','')}: {str(e)}")
           raise 


 
with DAG(
     'DDL_DAG', 
     schedule_interval=None, 
     start_date=pendulum.parse('2023-12-13'), 
     catchup=False,
     tags=['final', 'project', 'postgres']
     ) as dag:

     # Per file tasks
     for i, sql_file in enumerate(sql_files):
          create_tables = PythonOperator(
               task_id=f'make_tables_{i + 1}',
               python_callable=lambda file=sql_file: make_tables(file),
          )

create_tables  

 
 
