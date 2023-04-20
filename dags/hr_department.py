# My imports
import os

# DAG
from include.utils.MyFunctions import MyFunctions
from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule

# Operators
from airflow.operators.python import PythonOperator

# Sensor
from airflow.sensors.filesystem import FileSensor

# Other imports
from datetime import datetime, timedelta

# My Utils
mf = MyFunctions()

# Dag Development
default_args = {'owner': 'Romildo Marcusso',
                'retry': 5, 'retry_dealy': timedelta(minutes=5),
                'provide_context':True}

with DAG(
    dag_id='treatment_hr_department',
    description='The treatment of employee data to the HR department.',
    start_date=datetime(2023, 4, 17),
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
        python_callable=mf.getting_json_files,
    )
    
    getting_json_file_to_process_fail = PythonOperator(
        task_id='GettingJsonFileToProcessFail',
        python_callable=mf.send_message_telegram,
        op_kwargs={'status':False, 'message':'Task GettingJsonFileToProcess failed.'},
        trigger_rule=TriggerRule.ONE_FAILED
    )

    getting_schema_version = PythonOperator(
        task_id="GettingSchemaVersion",
        python_callable=mf.getting_schema_version,
    )

    getting_schema_version_fail = PythonOperator(
        task_id='GettingSchemaVersionFail',
        python_callable=mf.send_message_telegram,
        op_kwargs={'status':False, 'message':'Task GettingSchemaVersion failed.'},
        trigger_rule=TriggerRule.ONE_FAILED

    )

    processing_file = PythonOperator(
        task_id='ProcessingFile',
        python_callable=mf.persist_data,
    )

    processing_file_fail = PythonOperator(
        task_id='ProcessingFileFail',
        python_callable=mf.send_message_telegram,
        op_kwargs={'status':False, 'message':'Task ProcessingFile failed.'},
        trigger_rule=TriggerRule.ONE_FAILED
    )

    move_file = PythonOperator(
        task_id='MoveFileToProcessedFolder',
        python_callable=mf.move_to_processed,
        trigger_rule=TriggerRule.ONE_SUCCESS
    )

    move_file_fail = PythonOperator(
        task_id='MoveFileFail',
        python_callable=mf.send_message_telegram,
        op_kwargs={'status':False, 'message':'Task MoveFileToProcessedFolder failed.'},
        trigger_rule=TriggerRule.ALL_SKIPPED
    )

    pipeline_success = PythonOperator(
        task_id="PipelineRanSuccessfully",
        python_callable=mf.send_message_telegram,
        op_kwargs={'status':True, 'message': 'Pipeline processed with success'},
        trigger_rule=TriggerRule.ONE_SUCCESS
    )

    file_sensor >> getting_json_file_to_process >> getting_json_file_to_process_fail
    file_sensor >> getting_json_file_to_process >> getting_schema_version >> getting_schema_version_fail
    file_sensor >> getting_json_file_to_process >> getting_schema_version >> processing_file >> processing_file_fail
    file_sensor >> getting_json_file_to_process >> getting_schema_version >> processing_file >> move_file >> [move_file_fail, pipeline_success]
