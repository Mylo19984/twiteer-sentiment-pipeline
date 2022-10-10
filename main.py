from tweet_search import read_config, get_last_tweet_id, get_recent_user_tweets, save
from tweet_search import write_tweets_s3_bucket, write_tweets_s3_mongodb
from datetime import datetime, timedelta


config = read_config()

time_interval = True
if time_interval == True:
    begin_date = datetime.now().date() - timedelta(days=10)
    end_date = datetime.now().date()

# restructure
# query string for twitter user id
query_string = '44196397'
# bearer token
token = config['twitterApi']['btoken']
# start time of pulling tweets
start_time = F'{begin_date}T00:00:00Z'
print(start_time)
# end time of pulling tweets
end_time = F'{end_date}T12:00:00Z'
print(end_time)
# file name
file_name = 'el'
# highest tweet id, from which function will pull tweets
highest_tweet_id = get_last_tweet_id() if len(get_last_tweet_id()) > 0 else 1

df_search = get_recent_user_tweets(query_string, token, start_time, end_time, highest_tweet_id)
save(df_search, F'data/{file_name}.json')
write_tweets_s3_bucket(df_search)
write_tweets_s3_mongodb()