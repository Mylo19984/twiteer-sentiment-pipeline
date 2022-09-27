import tweet_search as search_tweet
from datetime import datetime, timedelta
import custom_func
import os

config = custom_func.read_config()
date_yesterday = datetime.now().date() - timedelta(days=1)
date_today = datetime.now().date() - timedelta(days=0)

# ovo ispod mora da se izmeni uzas je kod !!!
# query string for hashtag
query_string = '44196397'
# bearer token
token = config['twitterApi']['btoken']
# start time of pulling tweets
start_time = F'{date_yesterday}T00:00:00Z'
print(start_time)
# end time of pulling tweets
end_time = F'{date_today}T12:00:00Z'
print(end_time)
# file name, main part
file_name = 'elon_no1'

path_folder = config['twitterApi']['path']

# date time part for naming file
datetime_for_file_name = datetime.strptime(start_time, '%Y-%m-%dT%H:%M:%SZ')
date_for_file_name = datetime_for_file_name.date()

path = os.path.abspath(f'{path_folder}tweets_{file_name}_{date_for_file_name}.json')
df_search = search_tweet.get_recent_user_tweets(query_string, token, start_time, end_time)
custom_func.save(df_search, path)
search_tweet.write_tweets_s3_bucket(df_search)
search_tweet.write_tweets_s3_mongodb()