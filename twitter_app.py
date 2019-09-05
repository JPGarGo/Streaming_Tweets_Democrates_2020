'''
Biden

Credentials

tZ1rMPL5ilNdHQYDesRQWzclS (API key)

YYSwbP8Rdg1N5dfUCXmbWmCWT7tOhRYQUmuOXV2u9HZYU6EkD5 (API secret key)

Access token & access token secret
108469704-O3qKxmLhMVe1nvXkwojRYQdyE3s7Yn2dHhtSvQ7e (Access token)

8RZWMRYbx7UIQLlm9sm6MeasPYj2e31g2niL7CimifG8K (Access token secret)

'''

import socket
import sys
import requests
import requests_oauthlib 
import json

ACCESS_TOKEN = '108469704-O3qKxmLhMVe1nvXkwojRYQdyE3s7Yn2dHhtSvQ7e'
ACCESS_SECRET = '8RZWMRYbx7UIQLlm9sm6MeasPYj2e31g2niL7CimifG8K'
CONSUMER_KEY = 'tZ1rMPL5ilNdHQYDesRQWzclS'
CONSUMER_SECRET = 'YYSwbP8Rdg1N5dfUCXmbWmCWT7tOhRYQUmuOXV2u9HZYU6EkD5'

TCP_IP = "localhost"
TCP_PORT = 1236

my_auth = requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET,ACCESS_TOKEN, ACCESS_SECRET)

#Connect to twitter api for the search criteria for candidate Elizabeth Warren for the NYC area.
def get_tweets():
    url = 'https://stream.twitter.com/1.1/statuses/filter.json'
    query_data = [('language', 'en'), ('locations','-74,40,-73,41'),('track','Warren',)] 
    query_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in query_data]) 
    response = requests.get(query_url, auth=my_auth, stream=True) 
    print(query_url, response)
    return response

def send_tweets_to_spark(http_resp, tcp_connection): 
    for line in http_resp.iter_lines():
        try:
            full_tweet = json.loads(line)
            tweet_text = full_tweet['text']
            print("Tweet Text: " + tweet_text)
            print ("------------------------------------------") 
            tweet_data = bytes(tweet_text + "\n", 'utf-8')
            tcp_connection.send(tweet_data) 
        except:
            e = sys.exc_info()[0] 
            print("Error: %s" % e)
            

conn = None
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
print("Waiting for TCP connection...") 
conn, addr = s.accept()
print("Connected... Starting send tweets.") 
resp = get_tweets()
print("Connected... Sending tweets.") 
send_tweets_to_spark(resp, conn)
