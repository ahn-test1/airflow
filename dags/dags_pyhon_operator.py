from airflow.models.dag import DAG
import datetime
from airflow.operators.python import PythonOperator
import pendulum

with DAG(
    dag_id="dags_pyhon_operator",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    def select_fruit():
        fruit = ['APPLE','BANANA','ORANGE',]
        rand_int = rand.randint(0,3)
        print(fruit[rand_int])
        
    py_t1 = PythonOperator(
        task_id='py_t1'
        python_callable=select_fruit
    )
    
    py_t1