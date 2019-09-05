#---------------------------------------
from pyspark.sql import SparkSession
from pyspark.sql.functions import split
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from pyspark.sql.functions import udf
from time import sleep
import re
from geopy.geocoders import Nominatim
#---------------------------------------
import pandas as pd

#used nltk library to remove stopwords
import nltk
nltk.download('stopwords')
from nltk.corpus import stopwords

import sys
import requests

TCP_PORT = 1236

stop_words = stopwords.words("english")

# def aggregate_tags_count(new_values, total_sum): 
#     return sum(new_values) + (total_sum or 0)

def get_sql_context_instance(spark_context):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(spark_context) 
    return globals()['sqlContextSingletonInstance']

#We tried to sent the aggregate count of positive, negative and neutral sentiment of tweets to an HTML dashboard but were unable to succeed 
def send_df_to_dashboard(df):
    # extract the hashtags from dataframe and convert them into array
    top_tags = [str(t.hashtag) for t in df.select("final").collect()]
    # extract the counts from dataframe and convert them into array
    tags_count = [p.hashtag_count for p in df.select("Count").collect()]
    # initialize and send the data through REST API
    url = 'http://localhost:5001/updateData'
    request_data = {'label': str(top_tags), 'data': str(tags_count)}
    response = requests.post(url, data=request_data)


spark = SparkSession.builder.getOrCreate()


lines= spark.readStream.format('socket').option('host','localhost').option('port',TCP_PORT).load()

# Get Sentiment
def sentiment(x):
    if x>0:
        return('positive')
    elif x==0:
        return ('neutral')
    else:
        return ('negative')

#Calculate the sentiment score on the text of each tweet
analyser = SentimentIntensityAnalyzer()
sentiment_analyzer = udf(lambda text: analyser.polarity_scores(text))
compound_sentiment = udf(lambda score: score.get('compound'))
positive_sentiment = udf(lambda score: score.get('pos'))
negative_sentiment = udf(lambda score: score.get('neg'))
neutral_sentiment = udf(lambda score: score.get('neu'))
rounded = udf(lambda x: round(x))

lines = lines\
    .withColumn('Sentiment', sentiment_analyzer(lines.value))\

lines = lines\
    .withColumn('Positive', positive_sentiment(lines.Sentiment))\
    .withColumn('Negative', negative_sentiment(lines.Sentiment))\
    .withColumn('Neutral', neutral_sentiment(lines.Sentiment))\
    .withColumn('OverallSentiment', compound_sentiment(lines.Sentiment))\
    #.drop('value')\
    #.drop('Sentiment')

lines = lines.withColumn('final', rounded(lines.OverallSentiment))

# Start stream of tweets 
linesStream= lines\
    .writeStream\
    .trigger(processingTime='3 seconds')\
    .queryName("Tweets")\
    .format("memory")\
    .start()

# Analyzing streams
all_tweets = "SELECT * FROM Tweets" #Sentiment table
group = "SELECT final, COUNT(*) AS Count FROM Tweets group by final"  #Aggregate sentiment table

dashboard = spark.sql(group)

while True:
    print('***********************************************************')
    spark.sql(all_tweets).show()
    spark.sql(group).show()

    sleep(3) #Refresh table every 3 seconds

    
#--------------------------------------------------------------------------------------#

#send_df_to_dashboard(dashboard)
    