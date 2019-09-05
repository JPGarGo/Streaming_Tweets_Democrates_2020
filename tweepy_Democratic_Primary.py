import time
import re
import sys
import tweepy
import csv
import pandas as pd
import nltk
from nltk.stem.porter import *
from wordcloud import WordCloud, STOPWORDS
import matplotlib.pyplot as plt
import seaborn as sns
#from tweepy import OAuthHandler
from tweepy.auth import OAuthHandler
# sns.set()
# %matplotlib inline

import warnings 
warnings.filterwarnings("ignore", category=DeprecationWarning)

import requests
import json
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
analyser = SentimentIntensityAnalyzer()

#---------------------------------------------------------------------#

def sentiment_analyzer_scores(text):
    score = analyser.polarity_scores(text)
    lb = score['compound']
    if lb >= 0.05:
        return 1
    elif (lb > -0.05) and (lb < 0.05):
        return 0
    else:
        return -1

#---------------------------------------------------------------------#

def word_cloud(wd_list):
    stopwords = set(STOPWORDS)
    all_words = ' '.join([text for text in wd_list])
    wordcloud = WordCloud(
        background_color='white',
        stopwords=stopwords,
        width=1600,
        height=800,
        random_state=21,
        colormap='jet',
        max_words=50,
        max_font_size=200).generate(all_words)

    plt.figure(figsize=(12, 10))
    plt.axis('off')
    plt.imshow(wordcloud, interpolation="bilinear");

#---------------------------------------------------------------------#

access_token = '108469704-O3qKxmLhMVe1nvXkwojRYQdyE3s7Yn2dHhtSvQ7e'
access_token_secret = '8RZWMRYbx7UIQLlm9sm6MeasPYj2e31g2niL7CimifG8K'
consumer_key = 'tZ1rMPL5ilNdHQYDesRQWzclS'
consumer_secret = 'YYSwbP8Rdg1N5dfUCXmbWmCWT7tOhRYQUmuOXV2u9HZYU6EkD5'

auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth)

# tweets = api.user_timeline('Madrid', count=5, tweet_mode='extended')
# for t in tweets:
#     print(t.full_text)
#     print()

# class CustomStreamListener(tweepy.StreamListener):
#     def on_status(self, status):
#         if 'manchester united' in status.text.lower():
#             print(status.text)

#     def on_error(self, status_code):
#         print >> sys.stderr, 'Encountered error with status code:', status_code
#         return True # Don't kill the stream

#     def on_timeout(self):
#         print >> sys.stderr, 'Timeout...'
#         return True # Don't kill the stream

# sapi = tweepy.streaming.Stream(auth, CustomStreamListener())    
# sapi.filter(locations=[-6.38,49.87,1.77,55.81])

def twitter_stream_listener(file_name,
                            filter_track,
                            follow=None,
                            locations=None,
                            languages=None,
                            time_limit=20):
    class CustomStreamListener(tweepy.StreamListener):
        def __init__(self, time_limit):
            self.start_time = time.time()
            self.limit = time_limit
            # self.saveFile = open('abcd.json', 'a')
            super(CustomStreamListener, self).__init__()

        def on_status(self, status):
            if (time.time() - self.start_time) < self.limit:
                print(".", end="")
                # Writing status data
                with open(file_name, 'a') as f:
                    writer = csv.writer(f)
                    writer.writerow([
                        status.author.screen_name, status.created_at,
                        status.text
                    ])
            else:
                print("\n\n[INFO] Closing file and ending streaming")
                return False

        def on_error(self, status_code):
            if status_code == 420:
                print('Encountered error code 420. Disconnecting the stream')
                # returning False in on_data disconnects the stream
                return False
            else:
                print('Encountered error with status code: {}'.format(
                    status_code))
                return True  # Don't kill the stream

        def on_timeout(self):
            print('Timeout...')
            return True  # Don't kill the stream

    # Writing csv titles
    print(
        '\n[INFO] Open file: [{}] and starting {} seconds of streaming for {}\n'
        .format(file_name, time_limit, filter_track))
    with open(file_name, 'w') as f:
        writer = csv.writer(f)
        writer.writerow(['author', 'date', 'text'])

    streamingAPI = tweepy.streaming.Stream(
        auth, CustomStreamListener(time_limit=time_limit))
    streamingAPI.filter(
        track=filter_track,
        follow=follow,
        locations=locations,
        languages=languages,
    )
    f.close()

filter_track = ['trump', 'wall']
file_name = 'tweets_trump_wall.csv'
twitter_stream_listener (file_name, filter_track)