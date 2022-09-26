# import tweepy
import pandas as pd
import tweepy as tw
import json
import searchrecenttweetsConversation as search_conv
import searchrecenttweets as search_tweet
from datetime import datetime, timedelta
import helper
import os

config = helper.read_config()

#date_yesterday = datetime.now().date() - timedelta(days=1)
#date_today = datetime.now().date()
# iznad sam menjao 26072022
#
date_yesterday = datetime.now().date() - timedelta(days=1)
date_today = datetime.now().date() - timedelta(days=0)

# query string for hashtag
query_string = '#AZERO'
# query string for conversation id
query_string_conv = 'conversation_id:1543165093848006658'
# bearer token
token = config['twitterApi']['btoken']
# start time of pulling tweets
start_time = F'{date_yesterday}T00:00:00Z'
# end time of pulling tweets
end_time = F'{date_today}T00:00:00Z'
# file name, main part
file_name = 'azeroproj'

path_folder = config['twitterApi']['path']

query_int = 1544648592530591745

# date time part for naming file
datetime_for_file_name = datetime.strptime(start_time, '%Y-%m-%dT%H:%M:%SZ')
date_for_file_name = datetime_for_file_name.date()

# search of tweets based on hashtag
# df_search = search_tweet.get_recent_tweets(query_string, token, start_time, end_time)

# search of tweets based on conversation id
#df_search_conv = search_tweet.get_recent_tweets(query_string_conv, token, start_time, end_time)

# logic for getting all tweets of conversations based upon the hashtag
# list_of_conversation = search_tweet.get_conversation_id_for_search(df_search)

# print(len(list_of_conversation))

# df_full = pd.DataFrame()
#
# for l in list_of_conversation:
#      query_string_conv = F'conversation_id:{l}'
#      df = search_tweet.get_recent_tweets(query_string_conv, token, start_time, end_time)
#      df_full = pd.concat([df, df_full])
#
# df_search_and_conversation = pd.concat([df_search, df_full])

# saving dataframe to csv
# df_search.to_csv(F'{file_name}data_from{date_for_file_name}.csv', sep=';', encoding='utf-8')

#df_liked_tweets = search_tweet.get_tweet_likes(query_int, token, start_time, end_time)

#print(df_liked_tweets.head())

if __name__ == '__main__':
    curr_timestamp = int(datetime.timestamp(datetime.now()))
    path = os.path.abspath(f'{path_folder}tweets_{file_name}_{date_for_file_name}_{curr_timestamp}.csv')
    df_search = search_tweet.get_recent_tweets(query_string, token, start_time, end_time)
    helper.save(df_search, path)
