import findspark
findspark.init()
from pyspark.sql import SparkSession


if __name__ == '__main__':    
    # file_rdd = sc.textFile('/home/ogi/projects/end_to_end_final_project/data/distribution_centers.csv')
    # print("\n",file_rdd)
    # print("\n",type(file_rdd))
    # print("\n",dir(file_rdd))
    input_uri = "mongodb://localhost:27017/"
    output_uri = "mongodb://localhost:27017/"

    my_spark =  SparkSession\
                .builder\
                .appName("MyApp")\
                .config("spark.mongodb.input.uri", input_uri)\
                .config("spark.mongodb.output.uri", output_uri)\
                .config('spark.jars.packages','org.mongodb.spark:mongo-spark-connector_2.12:2.4.2')\
                .getOrCreate()
    
    data =  my_spark.read.format('csv')\
            .option('header','true')\
            .load('file:///home/ogi/projects/end_to_end_final_project/data/events.csv')
    data.show()
    data.write.format('mongo').mode('overwrite')\
        .option('database','final_project')\
        .option('collection','events')\
        .save() 
    my_spark.stop()
    