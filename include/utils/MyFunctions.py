# Import PySpark self Resources
from pyspark.sql.functions import *

# My Imports
import os
import shutil
from datetime import datetime
import hashlib
import pandas as pd
import requests

# Airflow Imports
from airflow.models import Variable

# PySpark Imports
from pyspark.sql import SparkSession

class MyFunctions(object):

    def __init__(self):
        self.spark = (SparkSession.builder
                      .config('spark.jars', 'include/driver/postgresql-42.6.0.jar')
                      .config('spark.driver.extraClassPath', 'include/driver/postgresql-42.6.0.jar')
                      .appName("MyProject").getOrCreate())

        self.host = Variable.get('host')
        self.port = Variable.get('port')
        self.database = Variable.get('database')
        self.username = Variable.get('username_secret')
        self.password = Variable.get('password_secret')

        self.url = f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"

    def rmtree_spark_directory(self):
        try:
            shutil.rmtree(f'{os.getcwd()}/spark-warehouse')
        except:
            print(f"'{os.getcwd()}/spark-warehouse' doesn\'t exists.")

    def send_message_telegram(self, ti, status=None, message=None, **context):
        file = ti.xcom_pull(key='json_file_to_process', task_ids=['GettingJsonFileToProcess'])[0]
        fileName = file.split('/')[-1]

        telegram_token = Variable.get('bot_token')
        telegram_chat_id = Variable.get('chat_id_secret')

        url = f'https://api.telegram.org/bot{telegram_token}/sendMessage'

        # format string for the timestamp
        format_str = '%Y-%m-%dT%H:%M:%S.%f%z'

        # convert the string timestamp to a datetime object
        datetime_obj = datetime.strptime(context['ts'], format_str)

        # new value with new format
        new_ts = datetime_obj.strftime("%m-%d-%Y %H:%M:%S")

        # getting the dag name in context
        dag_components = context['task_instance_key_str'].split('__')
        dag_name = dag_components[0]
        
        if not status:
            to_send_message = f'''
    # Dag name: {dag_name} processing at {new_ts}

    STATUS: Fail

    MESSAGE:
    {message}
    Filename: {fileName}
    '''
        else:
            to_send_message = f'''
    # Dag name: {dag_name} processing at {new_ts}

    STATUS: Success

    MESSAGE: {message}
    '''
        
        data = {'chat_id': telegram_chat_id, 'text': to_send_message}

        requests.post(url, data=data)


    def is_exist_table(self, table):
        # Evaluating inf the table exists
        is_exist_table = bool(self.spark.read.format('jdbc')
                .option('url', self.url)
                .option('dbtable', 'information_schema.tables')
                .option('user', self.username)
                .option('password', self.password)
                .option('driver', 'org.postgresql.Driver').load().filter(f'table_name == "{table}"').count())
        return is_exist_table
    

    def getting_json_files(self, ti):

        self.rmtree_spark_directory()
        
        list_files = [x for x in os.listdir(
            f'include/data/new/') if 'employees_details.json' in x]

        for file in list_files:
            file = f'include/data/new/{file}'
            break

        ti.xcom_push(key='json_file_to_process', value=file)


    def getting_schema_version(self, ti):
        file = ti.xcom_pull(key='json_file_to_process', task_ids=['GettingJsonFileToProcess'])[0]
        fileName = file.split('/')[-1]
       
        # Getting the schema
        schema_raw_data = str(self.spark.read.option('inferSchema',True).option('multiline','true').json(file).schema)

        current_no_space = schema_raw_data.replace(' ','')
        current_no_space = [i for i in current_no_space]
        current_no_space.sort()
        current_no_space = ''.join(current_no_space)

        current_hash_no_space = hashlib.sha256(current_no_space.encode('utf-8'))
        current_hash_no_space = current_hash_no_space.hexdigest()

        # Mounting the dataframe to save the history of schemas used.
        data = [{'id' : 1, 'schema' : schema_raw_data, 'version' : current_hash_no_space, 'file_name': fileName}]
        current_schema = self.spark.createDataFrame(data)
        current_schema = current_schema.select('id','schema','version','file_name').withColumn('load_timestamp', to_timestamp(lit(datetime.now().strftime("%Y%m%d %H:%M:%S")),'yyyyMMdd H:m:s'))
        current_schema.show()

        # Evaluating inf the table exists
        exist_table = self.is_exist_table('schema_history')

        # Get Current table in 
        if exist_table:
            table_history = (self.spark.read.format('jdbc').option('url', self.url)
                                .option('dbtable', 'schema_history')
                                .option('user', self.username)
                                .option('password', self.password)
                                .option('driver', 'org.postgresql.Driver').load())

            max_id = table_history.select(max(col('id')).alias('max_id')).collect()[0][0]
            new_id = max_id + 1

            new_value = [{'id':new_id, 'schema':schema_raw_data, 'version' : current_hash_no_space, 'file_name':fileName}]

            df_new_value = self.spark.createDataFrame(new_value)
            df_new_value = df_new_value.select('id','schema','version','file_name').withColumn('load_timestamp', to_timestamp(lit(datetime.now().strftime("%Y%m%d %H:%M:%S")),'yyyyMMdd H:m:s'))

            old_hash_schema = table_history.filter(f"id == ({max_id})").select('version').collect()[0][0]
            
            if old_hash_schema != current_hash_no_space:
                df_persist = table_history.unionAll(df_new_value)                

                df_persist.write.mode('overwrite').saveAsTable('tmp_schema_history')

                tb_persist = self.spark.table('tmp_schema_history').orderBy(col('id'))

                (tb_persist
                    .write
                    .mode('overwrite')
                    .format('jdbc')
                    .option('url', self.url)
                    .option('dbtable', 'schema_history')
                    .option('user', self.username)
                    .option('password', self.password)
                    .option('driver', 'org.postgresql.Driver')
                    .save())
                
        # # Creating Table
        else:
            print('New Table')
            (current_schema
                .write
                .format('jdbc')
                .option('url', self.url)
                .mode('overwrite')
                .option('dbtable', 'schema_history')
                .option('user', self.username)
                .option('password', self.password)
                .option('driver', 'org.postgresql.Driver')
                .save())


    def persist_data(self, ti):
        file = ti.xcom_pull(key='json_file_to_process', task_ids=['GettingJsonFileToProcess'])[0]

        employees_raw = (self.spark.read
                         .option('inferSchema', True)
                         .option('multiline', 'true').json(file))
        
        fileName = file.split('/')[-1]
        load_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        other_types = [column for column, datatype in employees_raw.dtypes if 'struct' not in datatype and 'array' not in datatype]
        struct_types = [f'{column}.*' for column, datatype in employees_raw.dtypes if 'struct' in datatype and 'array' not in datatype]
        array_types = [column for column, datatype in employees_raw.dtypes if 'array' in datatype]

        # Columns separating structs and array types
        new = other_types + struct_types + array_types

        second_change = employees_raw.select(new)

        for i in array_types:
            second_change = second_change.select('*',explode(i).alias(f'{i}_ex')).drop(i).withColumnRenamed(f'{i}_ex',i)
            second_change = second_change.select('*',f'{i}.*').drop(i)

        new_data = second_change.withColumn('file_name',lit(fileName)).withColumn('load_timestamp', to_timestamp(lit(load_timestamp), 'yyyy-MM-dd HH:mm:ss'))

        file_columns = new_data.schema
        file_columns = [(i.name, i.dataType) for i in file_columns]
        file_columns.sort()

        file_columns = {"name":file_columns}
        df_file_columns = pd.DataFrame(file_columns)

        # Evaluating inf the table exists
        exist_table = self.is_exist_table('employees')

        # Get Current table in 
        if exist_table:
            print('Persisting data...')
            employees = (self.spark.read.format('jdbc').option('url', self.url)
                            .option('dbtable', 'employees')
                            .option('user', self.username)
                            .option('password', self.password)
                            .option('driver', 'org.postgresql.Driver').load())

            table_columns = employees.schema
            table_columns = [(i.name, i.dataType) for i in table_columns]
            table_columns.sort()

            table_columns = {"name":table_columns}
            df_table_columns = pd.DataFrame(table_columns)

            different_columns = df_table_columns.merge(df_file_columns, on='name', how='outer', indicator=True).query("_merge not in ('both')")

            only_in_table = different_columns.query("_merge == 'left_only'")['name'].tolist()
            only_in_file = different_columns.query("_merge == 'right_only'")['name'].tolist()

            for column_name, column_type in only_in_table:
                new_data = new_data.withColumn(column_name, lit(None).cast(column_type))

            for column_name, column_type in only_in_file:
                employees = employees.withColumn(column_name,lit(None).cast(column_type))

            all_columns = employees.columns
            all_columns.sort()

            new_data = new_data.select(all_columns)
            employees = employees.select(all_columns)

            new_emp = ','.join([f"'{i[0]}'" for i in new_data.select('id').distinct().collect()])

            last_change_employees = employees.filter(f"id not in ({new_emp})")

            persist_data = last_change_employees.unionAll(new_data)

            persist_data.write.mode('overwrite').saveAsTable('tmp_employees')
            tb_persist_data = self.spark.table('tmp_employees')

            (tb_persist_data
                .write
                .mode('overwrite')
                .format('jdbc')
                .option('url', self.url)
                .option('dbtable', 'employees')
                .option('user', self.username)
                .option('password', self.password)
                .option('driver', 'org.postgresql.Driver')
                .save())
        # Persist the data united with previous data
        else:            
            print('Creating new table')
            (new_data
                .write
                .mode('overwrite')
                .format('jdbc')
                .option('url', self.url)
                .option('dbtable', 'employees')
                .option('user', self.username)
                .option('password', self.password)
                .option('driver', 'org.postgresql.Driver')
                .save())

    
    def move_to_processed(self, ti):
        file = ti.xcom_pull(key='json_file_to_process', task_ids=['GettingJsonFileToProcess'])[0]
        print(f'''

        Moving file 
        {file} 
        to
        {file.replace('new', 'processed')}
        
        ''')
        os.rename(file, file.replace('new', 'processed'))
        self.rmtree_spark_directory()
