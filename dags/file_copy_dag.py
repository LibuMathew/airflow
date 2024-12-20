from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import shutil
import os

# Function to copy the file
def copy_file(source_path: str, destination_path: str):
    if not os.path.exists(source_path):
        raise FileNotFoundError(f"Source file does not exist: {source_path}")
    os.makedirs(os.path.dirname(destination_path), exist_ok=True)
    shutil.copy(source_path, destination_path)
    print(f"File copied from {source_path} to {destination_path}")

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

# Instantiate the DAG
with DAG(
    dag_id='file_copy_dag',
    default_args=default_args,
    description='A DAG to copy a file from source to destination',
    schedule_interval=None,  # Run manually or update to a cron schedule if needed
    start_date=datetime(2023, 12, 1),
    catchup=False,
) as dag:

    # Task to copy a file
    copy_task = PythonOperator(
        task_id='copy_file_task',
        python_callable=copy_file,
        op_kwargs={
            # Update these paths as per your Docker setup
            'source_path': '/opt/airflow/data/source/file.txt',
            'destination_path': '/opt/airflow/data/destination/file.txt',
        },
    )
