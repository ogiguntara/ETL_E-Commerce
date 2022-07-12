#!python3

import findspark
findspark.init()
from pyspark.sql import SparkSession
import time


def input_to_mongo(name_list):
    input_uri = "mongodb://localhost:27017/"
    output_uri = "mongodb://localhost:27017/"

    my_spark =  SparkSession\
                .builder\
                .appName("MyApp")\
                .config("spark.mongodb.input.uri", input_uri)\
                .config("spark.mongodb.output.uri", output_uri)\
                .config('spark.jars.packages','org.mongodb.spark:mongo-spark-connector_2.12:2.4.2')\
                .getOrCreate()
    for file in name_list :
        start_time = time.time()
        data = my_spark.read.format('csv')\
                .option('header','true')\
                .load(f'file:///home/ogi/projects/end_to_end_final_project/data/{file}')\
                .limit(1000)
        
        data.write.format('mongo').mode('overwrite')\
                .option('database','final_project')\
                .option('collection',file[:-4])\
                .save()
                
        end_time = time.time()
        #log per file stored
        print(f"[Project Preparation MongoDB] collection '{file[:-4]}' has been created for {round(end_time-start_time,2)} second") 
    my_spark.stop()

    

if __name__ == '__main__':
    print(f"[Project Preparation PostgreSQL] start") 
    #file name list
    name_list = ['users.csv','inventory_items.csv','order_items.csv','orders.csv','events.csv',]
    input_to_mongo(name_list=name_list)
    print(f"[Project Preparation PostgreSQL] end") 
