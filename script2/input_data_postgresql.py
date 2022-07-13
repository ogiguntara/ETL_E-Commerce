#!python3

import pandas as pd
import connection
import json
from sqlalchemy import text
import time


def input_to_postgres(name_list):
    """
    tranform csv data into sql format, and store them in to postgresql
    """
    
    #open credential data
    with open ('data/credentials.json') as cred:
            credential = json.load(cred)
    #input credential data to create engine using connection file 
    postgre_auth = connection.PostgreSQL(credential['postgresql_source'])
    engine, engine_conn = postgre_auth.connect(conn_type='engine')
    
    #Create Schema
    schema = 'final_project'
    engine_conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS {schema};'))

    #itteration all over the name list
    for file in name_list:
        start_time = time.time()
        
        #read csv as pandas dataframe
        data = pd.read_csv(f'data/{file}')
        datacount=data.shape[0]
        #write to postgresql
        data.to_sql(name=file[:-4], con=engine ,if_exists='replace',index=False,schema=schema)
        
        end_time = time.time()
        #log per file stored
        print(f"[Project Preparation PostgreSQL] table '{file[:-4]}' ({datacount} rows) has been created for {round(end_time-start_time,2)} second") 
        
    #closing every last connection    
    engine.dispose()    
    

if __name__ == '__main__':
    print(f"[Project Preparation PostgreSQL] start") 
    #file name list
    name_list = ['distribution_centers.csv','employees.csv',]
    input_to_postgres(name_list=name_list)
    print(f"[Project Preparation PostgreSQL] end") 
        
       