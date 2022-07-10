#!python3
import pandas as pd
from connection.postgresql import PostgreSQL
import json
from sqlalchemy import text
import time

def ingest_to_postgres(file):

    with open ('data/credentials.json') as cred:
            credential = json.load(cred) 
    postgre_auth = PostgreSQL(credential['postgresql_source'])
    engine, engine_conn = postgre_auth.connect(conn_type='engine')
    engine_conn.execute(text('CREATE SCHEMA IF NOT EXISTS final_project;'))
        
    data = pd.read_csv(file)
    data.to_sql(name=file[5:-4], con=engine ,if_exists='replace',index=False,schema='final_project')
    print(f"[Project Scenario Preparation] table '{file[5:-4]}' has been created",end=" ")       


if __name__ == '__main__':
    name_list = ['distribution_centers.csv','employees.csv',]
    for data in name_list:
        start_time = time.time()
        ingest_to_postgres(f'data/{data}')
        end_time = time.time()
        print(f'for {start_time-end_time} second')