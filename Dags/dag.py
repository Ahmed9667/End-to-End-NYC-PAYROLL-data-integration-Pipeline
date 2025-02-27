from datetime import datetime , timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from Extraction import run_extraction
from Transform import run_transformation
from Load import run_load

my_args = {
    'owner' : 'airflow',
    'depends_on_past' : False ,
    'start_date' : datetime(2025, 2, 28),
    'email_on_failure': False ,
    'email_on_retry': False ,
    'retries' : 1 ,
    'retries_delay' : timedelta(minutes=1)
    
}

dags = DAG(
    'nyc',
    default_args = my_args

)

extract_task = PythonOperator(
        task_id='run_extraction',
        python_callable=run_extraction,
        provide_context=True,
)

transform_task = PythonOperator(
        task_id='run_transformation',
        python_callable=run_transformation,
        provide_context=True,
)


load_task = PythonOperator(
        task_id='run_load',
        python_callable=run_load,
        provide_context=True,
)

# Task dependencies
extract_task >> transform_task >> load_task