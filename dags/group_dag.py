from airflow import DAG
from airflow.operators.bash import BashOperator

from groups.group_downloads import download_tasks # This is the folder where the subdag file is placed
from groups.group_transforms import transform_tasks
 
from datetime import datetime
 
with DAG('group_dag', start_date=datetime(2022, 1, 1), 
    schedule_interval='@daily', catchup=False) as dag:

    downloads = download_tasks()

    check_files = BashOperator(
        task_id='check_files',
        bash_command='sleep 10'
    )
 
    transforms = transform_tasks()

    downloads >> check_files >> transforms
    # [download_a, download_b, download_c] >> check_files >> [transform_a, transform_b, transform_c]