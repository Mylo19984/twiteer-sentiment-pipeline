# import tweepy
import pandas as pd
import tweepy as tw
import json
# import searchrecenttweetsConversation as search_conv
import tweet_search as search_tweet
from datetime import datetime, timedelta
# import helper; uvezi ovo je za config parser
import os

config = helper.read_config()


date_yesterday = datetime.now().date() - timedelta(days=1)
date_today = datetime.now().date() - timedelta(days=0)

# query string for hashtag
query_string = ''
# query string for conversation id
query_string_conv = ''
# bearer token
token = config['twitterApi']['btoken']
# start time of pulling tweets
start_time = F'{date_yesterday}T00:00:00Z'
# end time of pulling tweets
end_time = F'{date_today}T00:00:00Z'
# file name, main part
file_name = ''

path_folder = config['twitterApi']['path']

# date time part for naming file
datetime_for_file_name = datetime.strptime(start_time, '%Y-%m-%dT%H:%M:%SZ')
date_for_file_name = datetime_for_file_name.date()

# search of tweets based on hashtag
# df_search = search_tweet.get_recent_tweets(query_string, token, start_time, end_time)

# search of tweets based on conversation id
#df_search_conv = search_tweet.get_recent_tweets(query_string_conv, token, start_time, end_time)

if __name__ == '__main__':
    curr_timestamp = int(datetime.timestamp(datetime.now()))
    path = os.path.abspath(f'{path_folder}tweets_{file_name}_{date_for_file_name}_{curr_timestamp}.csv')
    df_search = search_tweet.get_recent_tweets(query_string, token, start_time, end_time)
    helper.save(df_search, path)
