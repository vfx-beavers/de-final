import pendulum
import vertica_python
import logging
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


def fill_global_metrics(date):
    try:
        query = open("/lessons/sql/fill_global_metrics.sql").read() 
        f_query =query.format(count_date=date) 
        with vertica_python.connect(**vertica_conn_info) as connection:
            cur_vertica = connection.cursor()
            cur_vertica.execute(f_query)
            cur_vertica.connection.commit()
            cur_vertica.close()  
    except Exception as ex:
        logging.error(f"Fill_global_metrics error in global_metrics: {str(ex)}")
        raise 

with DAG(
    'vitrin_renew_cdm', 
    schedule_interval="@daily", 
    start_date=pendulum.parse('2022-10-01'), 
    end_date=pendulum.parse('2022-10-31'), 
    catchup=True
) as dag:
 
    fill_global_metrics_task = PythonOperator(
         task_id='fill_global_metrics',
         python_callable=fill_global_metrics,
         op_kwargs={"date": "{{ ds }}"},
     )
 
fill_global_metrics_task  
