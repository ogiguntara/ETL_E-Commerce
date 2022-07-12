from transform import transform_order_items, transform_products, transform_users
from connection.postgresql import PostgreSQL
import json
import os
import time
import pandas as pd
from sqlalchemy import text
from tables.data_warehouse import Base

base_path = os.getcwd()
filename_batch = "scripts/batch.txt"

with open (base_path+'/data/credentials.json') as cred:
        credential = json.load(cred)
#create engine dwh        
postgre_auth = PostgreSQL(credential['postgresql_data_warehouse'])
engine_dwh, engine_conn = postgre_auth.connect(conn_type='engine')
#create engine dl
postgre_auth = PostgreSQL(credential['postgresql_data_lake'])
engine_dl, engine_conn = postgre_auth.connect(conn_type='engine')

def create_schema():
    Base.metadata.create_all(engine_dwh)

def main():
    start_time=time.time()
    print('[Transfrom & Load] Start transform and load data')
    create_schema()
    print('[Transfrom & Load] data warehouse tables are created')    
    df_users = pd.read_sql_query(f'SELECT * FROM raw.users_raw',con=engine_dl)
    transform_users(df_users)\
        .to_sql('dim_users',con=engine_dwh,index=False,if_exists='replace')
    print('[Transfrom & Load] dim_users has been update')
    df_products = pd.read_sql_query(f'SELECT * FROM raw.products_raw',con=engine_dl)
    df_order_items = pd.read_sql_query(f'SELECT * FROM raw.order_items_raw',con=engine_dl)
    
    transform_products(df_products,df_order_items)\
        .to_sql('dim_products',con=engine_dwh,index=False,if_exists='replace')
    print('[Transfrom & Load] dim_products has been update')
    transform_order_items(df_order_items)\
        .to_sql('fact_transactions',con=engine_dwh,index=False,if_exists='replace')
    print('[Transfrom & Load] fact_transactions has been update')
    print('[Transfrom & Load] clean data has been store at Data Warehouse')
    print("[Transfrom & Load] Transformation & Load duration: {0:.2f} seconds".format(time.time() - start_time))
    
    