#python3

import json
from unicodedata import name
from hdfs import InsecureClient
import time
import pandas as pd
from sqlalchemy import create_engine
import connection

def postgresql_to_hdfs(tables_list):
    
    #open credential data
    with open ('data/credentials.json') as cred:
            credential = json.load(cred)
            
    #input credential data to create engine using connection file 
    postgre_auth = connection.PostgreSQL(credential['postgresql_source'])
    engine, engine_conn = postgre_auth.connect(conn_type='engine')
    
    #create hadoop client
    client = InsecureClient(credential['hadoop_data_lake'])
    
    #iteration over the tables_list
    for table in tables_list :
        start_time=time.time()
        
        #read postgresql data
        schema = 'final_project'
        data = pd.read_sql_table(table,con=engine,schema=schema)
        datacount = data.shape[0]
        
        #write data into hadoop file system
        hadoop_path = f'/FinalProject/{table}{time}.csv'
        with client.write(hadoop_path, encoding='utf-8') as writer:
            data.to_csv(writer)
            
        end_time=time.time()
        print(f'[Data Ingestion] data {table} ({datacount} rows) has been stored in hdfs for {end_time-start_time} second')
        
if __name__ == '__main__':
    print(f"[Data Ingestion] start")
    tables_list = ['distribution_centers','employees']
    postgresql_to_hdfs(tables_list)
    