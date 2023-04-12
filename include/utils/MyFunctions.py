# Import Pyself. Resources
from pyspark.sql.functions import *

# My Imports
import os
import shutil
from datetime import datetime

# PySpark Imports
from pyspark.sql import SparkSession

class MyFunctions(object):

    def __init__(self):
        self.spark = SparkSession.builder.config('spark.jars', 'include/utils/driver/PostgreSQL-42.6.0.jar').appName("MyProject").getOrCreate()

        self.host = "localhost"
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
        data = [{'schema':schema_raw_data, 'version': 1}]
        current_schema = self.spark.createDataFrame(data)
        current_schema.show()

        (current_schema
            .write
            .format('jdbc')
            .option('url', self.url)
            .mode('overwrite')
            .option('dbtable', 'schema_version')
            .option('user', self.username)
            .option('password', self.password)
            .option('driver', 'org.postgresql.Driver')
            .save())

        # The logic ahead is infinished because the trouble with the connection between Postgres and Airflow/PySpark
        # Previously I have already done something similar, but without the SQL Database envolved.
        
        # if not os.path.exists(f'{os.getcwd()}/spark-warehouse/ensurwave.db/schema_version'):
        #     (current_schema
        #         .write
        #         .saveAsTable('ensurwave.schema_version')
        #     )
        # else:
        #     print('Entrou no ELSE')
        #     self.spark.read.load(f'{os.getcwd()}/spark-warehouse/ensurwave.db/schema_version').filter(
        #         f'schema not in ("{schema_raw_data}")').write.mode('overwrite').saveAsTable('ensurwave.temp_table')

        #     tmp_table = self.spark.table('ensurwave.temp_table')
        #     tmp_table.show()

        #     version = tmp_table.select(max(tmp_table.version).alias('max_value'))
        #     new_version = int(version.collect()[0][0]+1)
            
        #     current_schema = current_schema.drop('version').withColumn('version', lit(new_version))

        #     psdf_persist = tmp_table.unionAll(current_schema)
        #     psdf_persist.show()

        #     (psdf_persist.write
        #         .mode('overwrite')
        #         .option('path', f'{os.getcwd()}/spark-warehouse/ensurwave.db/schema_version')
        #         .saveAsTable('ensurwave.temp_table'))

        #     self.spark.sql('drop table if exists ensurwave.temp_table')
        #     self.spark.table('ensurwave.schema_version').count()


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
            .option('dbtable', 'employee')
            .option('user', self.username)
            .option('password', self.password)
            .option('driver', 'org.postgresql.Driver')
            .save())


    
    def move_to_processed(self, ti):
        file = ti.xcom_pull(key='json_file_to_process', task_ids=['GettingJsonFileToProcess'])[0]
        os.rename(file, file.replace('new', 'processed'))