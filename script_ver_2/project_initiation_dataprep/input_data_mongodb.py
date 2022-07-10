import csv
import json
import pandas as pd
import pymongo
import time

# Function to convert a CSV to JSON
# Takes the file paths as arguments

def ingest_to_mongo(csvFilePath):
    #read csv transform to  dict
    if csvFilePath == 'data/events.csv':
        df = pd.read_csv(csvFilePath)
        data = df.to_dict(orient='records')
    else : 
        df = pd.read_csv(csvFilePath)
        data = df.to_dict(orient='records')
    #upload / insert many to mongo atlas
    myclient = pymongo.MongoClient('mongodb://localhost:27017')
    mydb = myclient["final_project"]
    mycol = mydb[f"{csvFilePath[5:-4]}"]
    mycol.delete_many({})
    mycol.insert_many(data)
    print(f"[Project Scenario Preparation] {csvFilePath[5:-4]} has been opload to mongodb",end=" ")
    

if __name__ == '__main__':
    name_list = ['users.csv','inventory_items.csv','order_items.csv','orders.csv','events.csv']
    for file_name in name_list:
        start_time = time.time()
        ingest_to_mongo(f'data/{file_name}') 
        end_time = time.time()
        print(f'for {start_time - end_time} second')
