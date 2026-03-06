from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# ── Simple functions for each task ──────────────────────

def say_hello():
    print("Hello from Task 1!")
    print(f"Current time: {datetime.now()}")

def say_world():
    print("Hello from Task 2!")
    print("CI/CD is working perfectly!")

def confirm_pipeline():
    print("All tasks completed!")
    print("Your GitHub → S3 CI/CD pipeline is working!")

# ── DAG Definition ──────────────────────────────────────

with DAG(
    dag_id="test_cicd_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,       # Updated parameter
    catchup=False,
    tags=["test", "cicd"],
) as dag:

    task1 = PythonOperator(
        task_id="say_hello",
        python_callable=say_hello,
    )

    task2 = PythonOperator(
        task_id="say_world",
        python_callable=say_world,
    )

    task3 = PythonOperator(
        task_id="confirm_pipeline",
        python_callable=confirm_pipeline,
    )

    task1 >> task2 >> task3