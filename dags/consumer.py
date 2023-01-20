from airflow import DAG, Dataset
from airflow.decorators import task

from datetime import datetime

# Here is a dataset
my_file = Dataset('/tmp/my_file.txt')
my_file_2 = Dataset('/tmp/my_file_2.txt')

with DAG(
    dag_id="consumer",
    schedule=[my_file, my_file_2], #now we can add all the Dataset expected before the DAG can run
    start_date=datetime(2023, 1, 5),
    catchup=False
):

    @task # no outlets parameter, so this task does not update the dataset
    def read_dataset():
        with open(my_file.uri, "r") as f:
            print(f.read())
    
    read_dataset()