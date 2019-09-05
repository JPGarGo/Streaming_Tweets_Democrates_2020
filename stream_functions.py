import time
import re
import pandas as pd
import numpy as np
import nltk
from nltk.stem.porter import *
from wordcloud import WordCloud, STOPWORDS
import matplotlib.pyplot as plt
import seaborn as sns
#sns.set()
#%matplotlib inline

import warnings 
#warnings.filterwarnings("ignore")

import requests
import json
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
analyser = SentimentIntensityAnalyzer()

#----------------------------------------------------------------------------------------------#
#Function to apply Vader sentiment analysis on the text to calculate the probability of the Positive, Negative or Neutral Sentiment
def sentiment_analyzer_scores(text):
    score = analyser.polarity_scores(text)
    lb = score['compound']
    if lb >= 0.05:
        return 1
    elif (lb > -0.05) and (lb < 0.05):
        return 0
    else:
        return -1

#----------------------------------------------------------------------------------------------#
#Plot wordcloud of most frequent words within a list of tweets
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

#----------------------------------------------------------------------------------------------#
#Function to remove special patterns such as # and @
def remove_pattern(input_txt, pattern):
    r = re.findall(pattern, input_txt)
    for i in r:
        input_txt = re.sub(i, '', input_txt)        
    return input_txt

#----------------------------------------------------------------------------------------------#
#Appply the remove patterns function depending on the identified pattern
def clean_tweets(lst):
    # remove twitter Return handles (RT @xxx:)
    lst = np.vectorize(remove_pattern)(lst, "RT @[\w]*:")
    # remove twitter handles (@xxx)
    lst = np.vectorize(remove_pattern)(lst, "@[\w]*")
    # remove URL links (httpxxx)
    lst = np.vectorize(remove_pattern)(lst, "https?://[A-Za-z0-9./]*")
    # remove special characters, numbers, punctuations (except for #)
    lst = np.core.defchararray.replace(lst, "[^a-zA-Z#]", " ")

    return lst

#----------------------------------------------------------------------------------------------#
# function to collect hashtags
def hashtag_extract(x):
    hashtags = []
    # Loop over the words in the tweet
    for i in x:
        ht = re.findall(r"#(\w+)", i)
        hashtags.append(ht)
    return hashtags

#----------------------------------------------------------------------------------------------#
#Plot count of aggregate sentiments classification
def anl_tweets(lst, title='Tweets Sentiment', engl=True ):
    sents = []
    for tw in lst:
        try:
            st = sentiment_analyzer_scores(tw)
            sents.append(st)
        except:
            sents.append(0)
    ax = sns.distplot(
        sents,
        kde=False,
        bins=3)
    ax.set(xlabel='Negative                Neutral                 Positive',
           ylabel='#Tweets',
          title="Tweets of @"+title)
    return sents

#----------------------------------------------------------------------------------------------#

# function to collect hashtags
def hashtag_extract(x):
    hashtags = []
    # Loop over the words in the tweet
    for i in x:
        ht = re.findall(r"#(\w+)", i)
        hashtags.append(ht)
    return hashtags

#----------------------------------------------------------------------------------------------#

#Apply all functions in the following order : Load Dataset, Clean Tweets , Plot Aggregate Sentiment of Tweets, Plot Most Frequent Hashtags
def Sentiment():
    biden_tweets = pd.read_csv("Biden.csv")
    biden_tweets['text'] =  clean_tweets(biden_tweets['text'])
    biden_tweets["Sentiment"] = anl_tweets(biden_tweets.text)
    # Words in positive tweets
    Biden_pos = biden_tweets['text'][biden_tweets['Sentiment'] == 1]
    word_cloud(Biden_pos)
    
    Biden_neg = biden_tweets['text'][biden_tweets['Sentiment'] == -1]
    word_cloud(Biden_neg)
    HT_positive = hashtag_extract(biden_tweets['text'][biden_tweets['Sentiment'] == 1])

    # extracting hashtags from negative tweets
    HT_negative = hashtag_extract(biden_tweets['text'][biden_tweets['Sentiment'] == -1])

    # unnesting list
    HT_positive = sum(HT_positive,[])
    
    HT_negative = sum(HT_negative,[])
    
    # Positive Tweets
    a = nltk.FreqDist(HT_positive)
    d = pd.DataFrame({'Hashtag': list(a.keys()),
                      'Count': list(a.values())})
    # selecting top 10 most frequent hashtags     
    d = d.nlargest(columns="Count", n = 10) 
    plt.figure(figsize=(16,5))
    plt.title('Most Frequent Positively related hashtags ')
    ax = sns.barplot(data=d, x= "Hashtag", y = "Count")
    ax.set(ylabel = 'Count')
    plt.show()
    
    # Negative Tweets
    b = nltk.FreqDist(HT_negative)
    e = pd.DataFrame({'Hashtag': list(b.keys()), 'Count': list(b.values())})
    # selecting top 10 most frequent hashtags
    e = e.nlargest(columns="Count", n = 10)   
    plt.figure(figsize=(16,5))
    plt.title('Most Frequent Negatively related hashtags ')
    ax = sns.barplot(data=e, x= "Hashtag", y = "Count")
    ax.set(ylabel = 'Count')
    plt.show()