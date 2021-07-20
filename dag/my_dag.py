from airflow import DAG
from airflow.operators.python import PythonOperator
from random import randint
from datetime import datetime

def choose_best():
    return 0
def trainingData():
    return randint(0, 10)


with DAG("my_dag", start_date=datetime().now(), schedule_interval="@daily", catchup=False) as dag:

    training_A = PythonOperator(task_id="training_A",python_callable=trainingData)
    training_B = PythonOperator(task_id="training_B",python_callable=trainingData)
    training_C = PythonOperator(task_id="training_C",python_callable=trainingData)

    choose_Best=BranchPythonOperator(task_id="choosing_model",python_callable=choose_Best)


