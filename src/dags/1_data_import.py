import pendulum
import vertica_python
import logging
import psycopg2
import io
from airflow import DAG
from airflow.models.variable import Variable
from airflow.operators.python import PythonOperator

 

# Airflow Postgres connection vars
postgres_conn={
     "host" : Variable.get('pg_host'),
     "port" : Variable.get('pg_port'),
     "user" : Variable.get('pg_user'),
     "password" : Variable.get('pg_pass'),
     "dbname": Variable.get('pg_dbname')
}

# Airflow Vertica connection vars
vertica_conn_info = {
     'host': Variable.get('VERTICA_HOST'),
     'port': Variable.get('VERTICA_PORT'),
     'user': Variable.get('VERTICA_USER'),
     'password': Variable.get('VERTICA_PASSWORD'),
     'database': '',
     'autocommit': True
}

def pg_to_vertica_fill(table: str, operation_ts: str, count_date)->None:
     try:
          with psycopg2.connect(**postgres_conn) as connect_to_postgresql:
               cur_postrgres = connect_to_postgresql.cursor()
               input = io.StringIO()
               cur_postrgres.copy_expert(f'''
                                         COPY (SELECT distinct * from public.{table} 
                                         WHERE {operation_ts}::date='{count_date}'::date-1 
                                         ORDER BY {operation_ts}) TO STDOUT;
                                         ''', input)
               cur_postrgres.close()
               
          with vertica_python.connect(**vertica_conn_info) as connection:

               cur_vertica = connection.cursor()  
               cur_vertica.execute(f"""
                                   DELETE FROM STV2023081241__STAGING.{ table } 
                                   WHERE {operation_ts}::date='{count_date}'::date-1
                                   """)
               connection.commit()
               cur_vertica.copy(f'''
                                COPY STV2023081241__STAGING.{table} 
                                FROM STDIN DELIMITER E'\t' NULL AS 'null' 
                                REJECTED DATA AS TABLE COPY_rejected;
                                ''', input.getvalue())
               cur_vertica.connection.commit()
 
     except Exception as ex:
          logging.error(f"Pg_to_vertica_fill error in {table}: {str(ex)}")
          raise 


with DAG(
     'pg_to_vertica_fill_DAG', 
     schedule_interval="@daily", 
     start_date=pendulum.parse('2022-10-01'), 
     end_date=pendulum.parse('2022-10-31'), 
     catchup=True,
     tags=['final', 'project', 'postgres', 'fill']
) as dag:

     # Data fill tasks
     load_transactions_task = PythonOperator(
         task_id='fill_transactions',
         python_callable=pg_to_vertica_fill,
         op_kwargs={'table': 'transactions' ,'operation_ts': 'transaction_dt', 'count_date': '{{ ds }}'},
      )
     load_currencies_task= PythonOperator(
         task_id='fill_currencies',
         python_callable=pg_to_vertica_fill,
         op_kwargs={'table': 'currencies' ,'operation_ts': 'date_update', 'count_date': '{{ ds }}'}, 
      )
  
load_transactions_task >> load_currencies_task 

