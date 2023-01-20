from airflow import DAG, Dataset
from airflow.decorators import task

from datetime import datetime

# Here is a dataset
my_file = Dataset('/tmp/my_file.txt')
my_file_2 = Dataset('/tmp/my_file_2.txt')

# We create the Producer DAG
# This DAG is in charge of updating the dataset that will trigger the consumer DAG
with DAG(
    dag_id="producer",
    schedule="@daily",
    start_date=datetime(2023, 1, 5),
):

    # Here is a task that uses the dataset
    #we indicate what task updates the dataset usning the outlets parameter
    @task(outlets=[my_file])
    def update_dataset():
        with open(my_file.uri, "a+") as f:
            f.write("producer update")

    @task(outlets=[my_file_2])
    def update_dataset_2():
        with open(my_file_2.uri, "a+") as f:
            f.write("producer update")
    
    update_dataset() >> update_dataset_2()