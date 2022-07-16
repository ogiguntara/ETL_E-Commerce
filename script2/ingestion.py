#python3

import json
import time
import findspark
findspark.init()
from pyspark.sql import SparkSession
from connection.postgresql import PostgreSQL
from datetime import datetime


def postgresql_to_hdfs(tables_list):
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
    
    for file in tables_list:
        start_time = time.time()
        #read csv as pandas dataframe
        schema = 'final_project'
        data = my_spark.read.jdbc(url=url, table=f'{schema}.{file}', properties=properties)
        datacount = data.count()
        
        #load data to hdfs        
        date = datetime.now().strftime("%Y%m%d")
        folder_name = f'FinalProject_{date}'
        data.coalesce(1).write.mode("overwrite").option("header",True).csv(f"hdfs:///{folder_name}/{file}.csv")

        end_time = time.time()
        #log per file stored
        print(f"[Data Ingestion] file '{file}.csv' ({datacount} rows) has been created for {round(end_time-start_time,2)} second")
        
        
        
        
        
if __name__ == '__main__':
    print(f"[Data Ingestion] start")
    print(f'[Data Ingestion] PostgreSQL to HDFS start')
    tables_list = ['distribution_centers','employees']
    postgresql_to_hdfs(tables_list)
    print(f'[Data Ingestion] PostgreSQL to HDFS end')
    print(f'[Data Ingestion] MongoDB to HDFS start')
    tables_list = ['distribution_centers','employees']
    print(f'[Data Ingestion] MongoDB to HDFS end')
    
    