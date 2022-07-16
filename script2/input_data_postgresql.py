#!python3

import json
import time
import os
import findspark
findspark.init()
from pyspark.sql import SparkSession
from sqlalchemy import text
from connection.postgresql import PostgreSQL


def input_to_postgres(name_list):
    """
    tranform csv data into sql format, and store them in to postgresql
    """
    
    #open credential data
    with open ('data/credentials.json') as cred:
            credential = json.load(cred)
    postgre_auth = PostgreSQL(credential['postgresql_source'])
    url, properties = postgre_auth.connect(conn_type='spark')
            
    #input credential data to create engine using connection file 
    my_spark = SparkSession \
    .builder \
    .appName('input_data_postgresql') \
    .config("spark.jars", "./postgresql-42.2.18.jar") \
    .config('spark.driver.extraClassPath', './postgresql-42.2.18.jar') \
    .getOrCreate()
    my_spark.sparkContext.setLogLevel('ERROR')
    
    #Create Schema 
    schema = 'final_project'
    postgre_auth = PostgreSQL(credential['postgresql_source'])
    enginedl, engine_conn = postgre_auth.connect(conn_type='engine')
    engine_conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS {schema};'))
    enginedl.dispose()
        
    file_path = os.getcwd()
    
    #itteration all over the name list
    for file in name_list:
        start_time = time.time()
        
        #read csv as pandas dataframe
        load_path = f'file://{file_path}/data/{file}'
        data = my_spark.read.format('csv')\
                .option('header','true')\
                .load(load_path)

        datacount = data.count()
        
        #write to postgresql
        data.write.jdbc(url=url, table=f'{schema}.{file[:-4]}', mode='overwrite', properties=properties)
        
        end_time = time.time()
        #log per file stored
        print(f"[Project Preparation PostgreSQL] table '{file[:-4]}' ({datacount} rows) has been created for {round(end_time-start_time,2)} second") 
        
    #closing every last connection    
    my_spark.stop()    
    

if __name__ == '__main__':
    print(f"[Project Preparation PostgreSQL] start") 
    #file name list
    name_list = ['distribution_centers.csv','employees.csv',]
    input_to_postgres(name_list=name_list)
    print(f"[Project Preparation PostgreSQL] end") 
        
       