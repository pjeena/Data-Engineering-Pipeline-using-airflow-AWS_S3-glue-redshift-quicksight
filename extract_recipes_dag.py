from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from extract_recipes_etl import run_recipes_etl

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 11, 8),
    'email': ['jeenapiyush101@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}


dag = DAG(
    'recipe_web_scraping_dag',
    default_args=default_args,
    description='ETL code to scrape recipes'
)

run_etl = PythonOperator(
    task_id='complete_web scraping_etl',
    python_callable=run_recipes_etl,
    dag=dag, 
)

run_etl
