# Import Pyself. Resources
from pyspark.sql.functions import *

# My Imports
import os
import shutil
from datetime import datetime
import hashlib

# PySpark Imports
from pyspark.sql import SparkSession

class MyFunctions(object):

    def __init__(self):
        self.spark = (SparkSession.builder
                      .config('spark.jars', 'include/driver/postgresql-42.6.0.jar')
                      .config('spark.driver.extraClassPath', 'include/driver/postgresql-42.6.0.jar')
                      .appName("MyProject").getOrCreate())

        self.host = "postgres"
        self.port = "5432"
        self.database = "ensurwave"
        self.username = "postgres"
        self.password = "postgres"
        self.url = f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"

    def getting_json_files(self, ti):
        list_files = [x for x in os.listdir(
            f'include/data/new/') if 'employees_details.json' in x]

        for file in list_files:
            file = f'include/data/new/{file}'
            break

        ti.xcom_push(key='json_file_to_process', value=file)

    def getting_schema_version(self, ti):
        file = ti.xcom_pull(key='json_file_to_process', task_ids=['GettingJsonFileToProcess'])[0]
       
        # Getting the schema
        schema_raw_data = str(self.spark.read.option('inferSchema',True).option('multiline','true').json(file).schema)

        # Mounting the dataframe to save the history of schemas used.
        current_no_space = schema_raw_data.replace(' ','')
        current_no_space = [i for i in current_no_space]
        current_no_space.sort()
        current_no_space = ''.join(current_no_space)

        current_hash_no_space = hashlib.sha256(current_no_space.encode('utf-8'))
        current_hash_no_space = current_hash_no_space.hexdigest()

        # Mounting the dataframe to save the history of schemas used.
        data = [{'id' : 1, 'schema' : schema_raw_data, 'hash' : current_hash_no_space, 'version' : 1}]
        current_schema = self.spark.createDataFrame(data)
        current_schema = current_schema.select('id','schema','hash','version').withColumn('load_timestamp', to_timestamp(lit(datetime.now().strftime("%Y%m%d %H:%M:%S")),'yyyyMMdd H:m:s'))

        # Get Current table in 
        try:
            table_history = (self.spark.read.format('jdbc').option('url', self.url)
                                .option('dbtable', 'schema_history')
                                .option('user', self.username)
                                .option('password', self.password)
                                .option('driver', 'org.postgresql.Driver').load())

            max_value = table_history.select(max(col('id')).alias('max_id'), max(col('version')).alias('max_version')).collect()[0]
            max_id, max_version = max_value
            new_id, new_version = max_id + 1, max_version + 1

            new_value = [{'id':new_id, 'schema':schema_raw_data, 'hash' : current_hash_no_space, 'version':new_version}]

            df_new_value = self.spark.createDataFrame(new_value)
            df_new_value = df_new_value.select('id','schema','hash','version').withColumn('load_timestamp', to_timestamp(lit(datetime.now().strftime("%Y%m%d %H:%M:%S")),'yyyyMMdd H:m:s'))

            old_hash_schema = table_history.filter(f"id == ({max_id})").select('hash').collect()[0][0]
            
            if old_hash_schema != current_hash_no_space:
                df_persist = table_history.unionAll(df_new_value)

                df_persist.write.mode('overwrite').saveAsTable('df_persist')

                tb_persist = self.spark.table('df_persist').orderBy(col('id'))

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
                shutil.rmtree(f'{os.getcwd()}/spark-warehouse')
        # Creating Table
        except:
            print('Nova Tabela')
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


    def create_parquet_file(self, ti):

        file = ti.xcom_pull(key='json_file_to_process', task_ids=['GettingJsonFileToProcess'])[0]

        partition_date = str(file.split("_")[0].split("/")[-1])

        employees_raw = self.spark.read.option('inferSchema',True).option('multiline','true').json(file)

        # Exploding Struct Types and separating array types
        other_types = [column for column, datatype in employees_raw.dtypes if 'struct' not in datatype and 'array' not in datatype]
        struct_types = [f'{column}.*' for column, datatype in employees_raw.dtypes if 'struct' in datatype and 'array' not in datatype]
        array_types = [column for column, datatype in employees_raw.dtypes if 'array' in datatype]

        # Columns separating structs and array types        
        new = other_types + struct_types + array_types

        second_change = employees_raw.select(new)

        # Adjusting dataframe to exploded columns
        for i in array_types:
            second_change = second_change.select('*',explode(i).alias(f'{i}_ex')).drop(i).withColumnRenamed(f'{i}_ex',i)
            second_change = second_change.select('*',f'{i}.*').drop(i)
        
        # Separating salary informations to join with the employee by ID
        salary = second_change.select(col('id').alias('id_emp'),'currency','type','value').groupBy('id_emp','currency').pivot('type').sum('value').drop('value','type').dropDuplicates()

        # The last change with the data to keep the employee in a line according informations on salary
        last_change = second_change.alias('emp').join(salary.alias('sal'), on=col('emp.id')==col('sal.id_emp'), how='inner').drop('id_emp','currency','type','value').dropDuplicates()
        last_change.show()

        # Informations to connect to localhost Postgres
        host = "localhost"
        port = "5432"
        database = "ensurwave"
        username = "postgres"
        password = "postgres"
        url = f"jdbc:postgresql://{host}:{port}/{database}"

        # This part of code works out of the Airflow environment
        (last_change
            .write
            .format('jdbc')
            .option('url', self.url)
            .mode('overwrite')
            .option('dbtable', 'lucas_new_host')
            .option('user', self.username)
            .option('password', self.password)
            .option('driver', 'org.postgresql.Driver')
            .save())


    
    def move_to_processed(self, ti):
        file = ti.xcom_pull(key='json_file_to_process', task_ids=['GettingJsonFileToProcess'])[0]
        os.rename(file, file.replace('new', 'processed'))