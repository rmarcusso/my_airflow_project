# My imports
import os

# DAG
from include.utils.MyFunctions import MyFunctions
from airflow import DAG

# Operators
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Sensor
from airflow.sensors.filesystem import FileSensor

# Other imports
from datetime import datetime, timedelta

# My Utils
mf = MyFunctions(location='airflow')

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
    
    # getting_schema_version_dockerfile = PythonOperator(
    #     task_id="GettingSchemaVersionDockerfile",
    #     python_callable=MyFunctions(location='dockerfile').getting_schema_version
    # )
    
    getting_schema_version_airflow = PythonOperator(
        task_id="GettingSchemaVersionAirflow",
        python_callable=MyFunctions(location='airflow').getting_schema_version
    )
    
    # getting_schema_version_together = PythonOperator(
    #     task_id="GettingSchemaVersionTogether",
    #     python_callable=MyFunctions(location='together').getting_schema_version
    # )


    # spark_submit_operator = SparkSubmitOperator(
    #     task_id="my_task",
    #     application=f'{os.getcwd()}/pyspark/script.py',
    #     name="my_spark_job",
    #     executor_memory="2g",
    #     driver_memory="1g",
    #     jars=f'{os.getcwd()}/jars/postgresql-42.6.0.jar'
    #     # conf={
    #     #     'jars': 'include/driver/PostgreSQL-42.6.0.jar',
    #     #     'executor.extraClassPath': 'include/driver/PostgreSQL-42.6.0.jar',
    #     #     'executor.extraLibrary': 'include/driver/PostgreSQL-42.6.0.jar',
    #     #     'driver.extraClassPath': 'include/driver/PostgreSQL-42.6.0.jar',
    #     # }
    # )

    spark_submit_bash = BashOperator(
        task_id='GettingSchemaVersionBash',
        bash_command=f'spark-submit --jars ~/pyspark/jars/postgresql-42.6.0.jar {os.getcwd()}/pyspark/script.py'
    )

    # processing_file = PythonOperator(
    #     task_id='ProcessingFile',
    #     python_callable=mf.create_parquet_file
    # )

    # move_file = PythonOperator(
    #     task_id='MoveFileToProcessedFolder',
    #     python_callable=mf.move_to_processed
    # )

    # move_file = PythonOperator(
    #     task_id='MoveFileToProcessingFolder',
    #     python_callable=mf.move_to_processed
    # )

    file_sensor >> getting_json_file_to_process >> [spark_submit_bash, getting_schema_version_airflow]

    # spark_submit_operator = SparkSubmitOperator(
    #     task_id="my_task",
    #     application=f'{os.getcwd()}/pyspark/script.py',
    #     name="my_spark_job",
    #     executor_memory="2g",
    #     driver_memory="1g",
    #     jars='/home/astro/jars/postgresql-42.6.0.jar'
    #     # conf={
    #     #     'jars': 'include/driver/PostgreSQL-42.6.0.jar',
    #     #     'executor.extraClassPath': 'include/driver/PostgreSQL-42.6.0.jar',
    #     #     'executor.extraLibrary': 'include/driver/PostgreSQL-42.6.0.jar',
    #     #     'driver.extraClassPath': 'include/driver/PostgreSQL-42.6.0.jar',
    #     # }
    # )

    # spark_submit_operator = BashOperator(
    #     task_id='SparkSubmitBash',
    #     bash_command=f'spark-submit --jars /home/astro/jars/postgresql-42.6.0.jar {os.getcwd()}/pyspark/script.py'
    # )

    # file_sensor >> spark_submit_operator
