from pyspark.sql import SparkSession

# Import PySpark Resources
from pyspark.sql.functions import *

# My Imports
import os
import shutil
from datetime import datetime

spark = (SparkSession.builder
    .appName("Postgres data loading")
    .getOrCreate())


host = "localhost"
port = "5433"
database = "ensurwave"
username = "postgres"
password = "postgres"
url = f"jdbc:postgresql://{host}:{port}/{database}"
properties = {
    "user": username,
    "password": password
}

# # Create a PySpark dataframe
employees_raw = spark.read.option('inferSchema',True).option('multiline','true').json('script/data/new/20230331_employees_details.json')

# employees_raw.show(truncate=False)
# employees_raw.printSchema()

other_types = [column for column, datatype in employees_raw.dtypes if 'struct' not in datatype and 'array' not in datatype]
struct_types = [f'{column}.*' for column, datatype in employees_raw.dtypes if 'struct' in datatype and 'array' not in datatype]
array_types = [column for column, datatype in employees_raw.dtypes if 'array' in datatype]

# Columns separating structs and array types        
new = other_types + struct_types + array_types

second_change = employees_raw.select(new)

for i in array_types:
    second_change = second_change.select('*',explode(i).alias(f'{i}_ex')).drop(i).withColumnRenamed(f'{i}_ex',i)
    second_change = second_change.select('*',f'{i}.*').drop(i)
    
salary = second_change.select(col('id').alias('id_emp'),'currency','type','value').groupBy('id_emp','currency').pivot('type').sum('value').drop('value','type').dropDuplicates()

last_change = second_change.alias('emp').join(salary.alias('sal'), on=col('emp.id')==col('sal.id_emp'), how='inner').drop('id_emp','currency','type','value').dropDuplicates()
last_change.show()

# Escreva os dados de volta no PostgreSQL
# last_change.write.jdbc(url=url, table="mytable_transformed", mode="overwrite", properties=properties).save()

(last_change
    .write
    .format('jdbc')
    .option('url', url)
    .option('dbtable', 'ludmila')
    .option('user', username)
    .option('password', password)
    .mode('append')
    .option('driver', 'org.postgresql.Driver')
    .save())

    # .mode('overwrite')
# Pare a sessão Spark
spark.stop()
