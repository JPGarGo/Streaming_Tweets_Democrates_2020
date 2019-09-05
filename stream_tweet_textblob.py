from pyspark import SparkConf,SparkContext 
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SQLContext
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
import pandas as pds
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

def collect_rdd(time, rdd):
    try:
        lis = rdd.collect()
        if (len(lis) > 0):
            print(lis[0])
            df = pd.DataFrame(columns=['time', 'text'])
            add_row(df, [str(time),lis[0]]) 

            # Create file unless exists, otherwise append
            # Add header if file is being created, otherwise skip it
            with open("Warren.csv", 'a') as f:
                df.to_csv(f, header=f.tell() == 0, index=False)
            print("--------- APPENDED ONE TWEET-------")

    except:
        e = sys.exc_info()[0] 
        print("Error: %s" % e)

import findspark
findspark.init()
import pyspark as ps
import warnings
from pyspark import SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import *


try:
    # create SparkContext on all CPUs available: in my case I have 4 CPUs on my laptop
    conf = SparkConf().setAll([('spark.executor.memory', '8g'),('spark.driver.memory','8g')]) 
    sc = ps.SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    print("Just created a SparkContext")
except ValueError:
    warnings.warn("SparkContext already exists in this scope")
    
sc.master

df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('/Users/rafaelhernandez/Documents/GitHub/Streaming_Tweets_Democrats_2020/train_sentiment.csv')

test_df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('/Users/rafaelhernandez/Documents/GitHub/Streaming_Tweets_Democrats_2020/Biden.csv')

df = df.dropna()

df = df.withColumn("label", df["label"].cast(IntegerType()))

df.dtypes

(train_set, val_set, test_set) = df.randomSplit([0.98, 0.01, 0.01], seed = 2000)

from pyspark.ml.feature import HashingTF, IDF, Tokenizer, CountVectorizer
from pyspark.ml.feature import StringIndexer
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import CountVectorizer
from pyspark.sql.types import *
from pyspark.ml import PipelineModel

tokenizer = Tokenizer(inputCol="text", outputCol="words")
cv = CountVectorizer(vocabSize=2**16, inputCol="words", outputCol='cv')
idf = IDF(inputCol='cv', outputCol="features", minDocFreq=5) #minDocFreq: remove sparse terms
label_stringIdx = StringIndexer(inputCol = "label", outputCol = "label")
rf = RandomForestClassifier()

#df = df.withColumn("label", df["label"].cast(IntegerType()))
train_set = train_set.dropna()

pipeline = Pipeline(stages=[tokenizer, cv, idf, rf])

pipelineFit = pipeline.fit(train_set)

rf_predictions = pipelineFit.transform(test_df)

rf_predictions.show()

pipeline.write().overwrite().save('pipelineFit')
#-------------------------------------------------------------------------------------------------------------------------------#

def apply_model(rdd):
    try:
        lis = rdd.collect()
        if (len(lis) > 0):
            print(lis[0])
            df = pd.DataFrame(columns=['time', 'text'])
            add_row(df, [str(time),lis[0]]) 

            # Create file unless exists, otherwise append
            # Add header if file is being created, otherwise skip it
            with open("Warren.csv", 'a') as f:
                df.to_csv(f, header=f.tell() == 0, index=False)
            print("--------- APPENDED ONE TWEET-------")

    except:
        e = sys.exc_info()[0] 
        print("Error: %s" % e)
    prediction = pipelineFit.transform(lis)
    selected = sentiment.select("prediction")
    return selected
        
#-------------------------------------------------------------------------------------------------------------------------------#       
# create spark configuration
# conf = SparkConf() 
# conf.setAppName("TwitterStreamApp")

# create spark context with the above configuration 
# sc = SparkContext(conf=conf) 
sc.setLogLevel("ERROR")

# create the Streaming Context from the above spark context with interval size 2 seconds 
ssc = StreamingContext(sc, 2)

# setting a checkpoint to allow RDD recovery 
ssc.checkpoint("checkpoint_TwitterApp")

# read data from port 1235
dataStream = ssc.socketTextStream("localhost",TCP_PORT)

#dataStream.foreachRDD(collect_rdd)
dataStream.foreachRDD(apply_model)

# def count_rdd(time, rdd):
#     print("----------- %s -----------" % str(time)) 
#     try:
#         # Get spark sql singleton context from the current context
#         sql_context = get_sql_context_instance(rdd.context)

#         # convert the RDD to Row RDD
#         row_rdd = rdd.map(lambda w: Row(word=w[0], word_count=w[1])) 
        
#         # create a DF from the Row RDD
#         words_df = sql_context.createDataFrame(row_rdd)

#         # Register the dataframe as table
#         words_df.registerTempTable("words")

#         # get the top 10 hashtags from the table using SQL and print them
#         word_counts_df = sql_context.sql("select word, word_count from words order by word_count desc limit 10")
#         word_counts_df.show() 
#         word_counts_df.toPandas().to_csv('biden_word_count.csv') # Export into a CSV
#     except:
#         e = sys.exc_info()[0] 
#         print("Error: %s" % e)        
        
# words = dataStream.flatMap(lambda line: line.split(" ")) 

# #Convert the words in lower case and remove stop words from stop_words
# filtered_words = words.filter(lambda x: x.lower() not in stop_words)

# # filter the words to get only hashtags, then map each hashtag to be a pair of (hashtag,1) 
# #hashtags = words.filter(lambda w: '#' in w).map(lambda x: (x, 1))
# # Count each word in each batch
# pairs = filtered_words.map(lambda word: (word, 1))
# wordCounts = pairs.reduceByKeyAndWindow(lambda x, y: int(x) + int(y), 60, 30)

# # adding the count of each hashtag to its last count
# tags_totals = wordCounts.updateStateByKey(aggregate_tags_count)
# tags_totals.foreachRDD(count_rdd) 
# tags_totals.pprint()

# start the streaming computation 
ssc.start()
# wait for the streaming to finish 
ssc.awaitTermination()