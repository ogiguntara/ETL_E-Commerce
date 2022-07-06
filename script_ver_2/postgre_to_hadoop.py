#!python3

import json
from unicodedata import name
from hdfs import InsecureClient
import time
import pandas as pd
from sqlalchemy import create_engine

def save_to_hdfs(table):
    engine = create_engine('postgresql://postgres:12345@localhost:5432/digitalskola')
    client = InsecureClient("http://localhost:9870")
    df = pd.read_sql_table(table,con=engine,schema='final_project')
    with client.write(f'/FinalProject/{table}.csv', encoding='utf-8') as writer:
        df.to_csv(writer)

if __name__ == '__main__':
    tables_list = ['distribution_centers','employees']
    for table in tables_list:
        start_time=time.time()
        save_to_hdfs(table)
        end_time=time.time()
        print(f'{table} has been saved in hdfs, runtime : {start_time-end_time}')
        
    