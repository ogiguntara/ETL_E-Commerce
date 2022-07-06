#!pyhton3
import pymongo
import pandas as pd
from hdfs import InsecureClient
import time 

def save_to_hdfs(collection):
    client_mongo = pymongo.MongoClient('mongodb+srv://ogi:ogi@cluster0.2zf4p.mongodb.net')
    mydb = client_mongo["final_project"]
    mycol = mydb[collection]
    data = mycol.find()
    df = pd.DataFrame(data[:])
    client_hdfs = InsecureClient("http://localhost:9870")
    with client_hdfs.write(f'/FinalProject/{collection}.csv', encoding='utf-8') as writer:
        df.to_csv(writer)
    client_mongo.close()
if __name__ == '__main__':
    collections_list = ['users','inventory_items','order_items','orders','events']
    for collection in collections_list:
        start_time = time.time()
        save_to_hdfs(collection)
        end_time = time.time()
        print(f'{collection} has been saved to hdfs, runtime : {start_time - end_time} second')
    