from airflow import DAG
fromairflow.operators.python import PythonOperator
from datetime import datetime
import subprocess

#Task function
def fetch_data():
    subprocess.run(["python","/opt/airflow/scripts/fetch_crypto_data.py"])
    
def stream_kafka():
    subprocess.run(["python", "/opt/airflow/scripts/kakfka_producer.py"])
    
#Dag definition
with DAG(
    dag_id="crypto_pipeline",
    start_date=datetime(2026,5,22),
    schedule= "@hourly",
    catchup = False
) as dag:
    
    task1 =PythonOperator(
        task_id ="fetch_crypto_data",
        python_callable =fetch_data
    )
    
    task2= PythonOperator(
        task_id ="stream_to_kafka",
        python_callable = stream_kafka
    )
    
    task1 >> task2