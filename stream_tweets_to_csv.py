from pyspark import SparkConf,SparkContext 
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SQLContext
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
#---------------------------------------

#---------------------------------------
import pandas as pd
import nltk
nltk.download('stopwords')
from nltk.corpus import stopwords

import sys
import requests

TCP_PORT = 1236

stop_words = stopwords.words("english")

def aggregate_tags_count(new_values, total_sum): 
    return sum(new_values) + (total_sum or 0)

def get_sql_context_instance(spark_context):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(spark_context) 
    return globals()['sqlContextSingletonInstance']

def add_row(df, row):
    df.loc[-1] = row
    df.index = df.index + 1  
    return df.sort_index()

#Function to collect the tweets and add them to a csv file
def collect_rdd(time, rdd):
    try:
        lis = rdd.collect()
        if (len(lis) > 0):
            print(lis[0])
            df = pd.DataFrame(columns=['time', 'text'])
            add_row(df, [str(time),lis[0]]) 

            # Create file unless exists, otherwise append
            # Add header if file is being created, otherwise skip it
            with open("Biden.csv", 'a') as f:
                df.to_csv(f, header=f.tell() == 0, index=False)
            print("--------- APPENDED ONE TWEET-------")

    except:
        e = sys.exc_info()[0] 
        print("Error: %s" % e)

# create spark configuration
conf = SparkConf() 
conf.setAppName("TwitterStreamApp")

# create spark context with the above configuration 
sc = SparkContext(conf=conf) 
sc.setLogLevel("ERROR")

# create the Streaming Context from the above spark context with interval size 2 seconds 
ssc = StreamingContext(sc, 2)

# setting a checkpoint to allow RDD recovery 
ssc.checkpoint("checkpoint_TwitterApp")

# read data from port 1235
dataStream = ssc.socketTextStream("localhost",TCP_PORT)

dataStream.foreachRDD(collect_rdd)

# start the streaming computation 

ssc.start()
# wait for the streaming to finish 
ssc.awaitTermination()
#-------------------------------------------------------------------------------------------------#


    
#--------------------------------------------------------------------------------------#
