#!python3

import os
import pandas as pd
import json
from zipfile import ZipFile
import time
from sqlalchemy import text
from tables.data_warehouse import Base
#import script
from connection.postgresql import PostgreSQL

#Path
base_path = os.getcwd()
source_path = f"{base_path}/data/Dataset Final Project - DE.zip"
filename_batch = "scripts/batch.txt"

def batch_ingest(period,data):
        if period == 1:
            data = data.iloc[0:2000,:]
        elif period == 2:
            data = data.iloc[2000:4000,:]
        elif period == 3:
            data = data.iloc[4000:6000,:]
        else:
            data = pd.DataFrame([])
        data.columns= [x.lower() for x in data.columns.to_list()]
        return data
    
def ingestion_to_data_lake():   
    with ZipFile(source_path,mode='r') as zipfile:
        names_list = zipfile.namelist()
        #hapus selain .csv dari name list
        for a in names_list:
            if '.csv' not in a:
                names_list.remove(a)
            else : pass
        print(f"\nFile name list : \n\n{names_list}\n")
        #postgresql connect
        with open (base_path+'/data/credentials.json') as cred:
            credential = json.load(cred) 
        postgre_auth = PostgreSQL(credential['postgresql_data_lake'])
        enginedl, engine_conn = postgre_auth.connect(conn_type='engine')
        engine_conn.execute(text('CREATE SCHEMA IF NOT EXISTS raw;'))
        #data ingest events       
        period = int(open(filename_batch,"r").read()) 
        a ='events.csv'
        if period > 3 :
            period = 1            
            df=pd.read_csv(zipfile.open(a))
            batch_ingest(period,df).to_sql(name=a[:-4]+'_raw_'+str(period),con=enginedl,if_exists='replace',index=False,schema='raw')
            print(f"[Ingestion] {a} has been stored to data lake...")
            with open(filename_batch,'w') as file:
                file.write(str(period+1)) 
        else :            
            df=pd.read_csv(zipfile.open(a))
            batch_ingest(period,df).to_sql(name=a[:-4]+'_raw_'+str(period),con=enginedl,if_exists='replace',index=False,schema='raw')
            print(f"[Ingestion] {a} has been stored to data lake...")
            with open(filename_batch,'w') as file:   
                file.write(str(period+1))
        #data ingest other
        for a in names_list:
            df=pd.read_csv(zipfile.open(a))
            batch_ingest(period,df).to_sql(name=a[:-4]+'_raw',con=enginedl,if_exists='replace',index=False,schema='raw')
            print(f"[Ingestion] {a} has been stored to data lake...")
            
        
        
    
def main():
    start_time=time.time()
    period = int(open(filename_batch,"r").read())
    if period > 3:
        period = 1
    print(f"[Ingestion] Start Batch Ingest Period {period}")
    print(f"[Ingestion] Extract data from {source_path}")
    ingestion_to_data_lake()       
    print("[Ingestion] Data Ingestion Done")
    print("[Ingestion] Ingestion to data lake period "+str(period)+" duration: {0:.2f} seconds".format(time.time() - start_time))