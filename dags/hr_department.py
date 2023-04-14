# My imports
import os

# DAG
from include.utils.MyFunctions import MyFunctions
from airflow import DAG

# Operators
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

# Sensor
from airflow.sensors.filesystem import FileSensor

# Other imports
from datetime import datetime, timedelta

# My Utils
mf = MyFunctions()
# Dag Development
default_args = {'owner': 'Romildo Marcusso',
                'retry': 5, 'retry_dealy': timedelta(minutes=5)}

with DAG(
    dag_id='treatment_hr_department',
    description='The treatment of employee data to the HR department.',
    start_date=datetime(2023, 1, 19),
    # schedule_interval=timedelta(seconds=10),
    schedule_interval='@once',
    max_active_runs=1,
    tags=['Challenge Insurwave'],
    catchup=False,
    default_args=default_args
) as dag:

    file_sensor = FileSensor(
        task_id='WaitingForJsonFile',
        fs_conn_id='getting_json_file',
        filepath='*_employees_details.json',
        poke_interval=10
    )

    getting_json_file_to_process = PythonOperator(
        task_id='GettingJsonFileToProcess',
        python_callable=mf.getting_json_files
    )
    
    getting_schema_version = PythonOperator(
        task_id="GettingSchemaVersion",
        python_callable=mf.getting_schema_version
    )

    # processing_file = PythonOperator(
    #     task_id='ProcessingFile',
    #     python_callable=mf.create_parquet_file
    # )

    # move_file = PythonOperator(
    #     task_id='MoveFileToProcessedFolder',
    #     python_callable=mf.move_to_processed
    # )

    file_sensor >> getting_json_file_to_process >> getting_schema_version # >> processing_file >> move_file
