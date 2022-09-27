import pandas
import tweepy
import time
import pandas as pd
import numpy as np
import re
import boto3
import json
import requests
import configparser
import time

def get_recent_user_tweets (query_string, token, start_time, end_time):
    """
    gets data frame of tweets based on the search string
    search string can either be hashtag or conversation id
    returns data frame of tweets
    """

    hoax_tweets = []

    client = tweepy.Client(bearer_token=token)

    for response in tweepy.Paginator(client.get_users_tweets,
                                 id = query_string,
                                 user_fields = ['username', 'public_metrics', 'description', 'location'],
                                 tweet_fields = ['created_at', 'geo', 'public_metrics', 'text', 'conversation_id'],
                                 media_fields = ['media_key', 'type'],
                                 expansions = ['author_id', 'referenced_tweets.id', 'attachments.media_keys'],
                                 start_time = start_time,
                                 end_time = end_time,
                             max_results=100):


        time.sleep(0.5)
        hoax_tweets.append(response)

    result = []
    user_dict = {}
    media_dict = {}

# get responses from tweet call
    if response[0] != None:
        for response in hoax_tweets:
            print(response)
    # get users data from tweet call
            for user in response.includes['users']:
                user_dict[user.id] = {'username': user.username,
                              'followers': user.public_metrics['followers_count'],
                              'tweets': user.public_metrics['tweet_count'],
                              'description': user.description,
                              'location': user.location,
                              #'imageUrl': user.profile_image_url
                             }

            # get media files if exists
            try:
                for m in response.includes['media']:
                    media_dict[m.media_key] = {'type': m.type
                                            }
            except KeyError:
                pass


            for tweet in response.data:
                print(tweet)
                print(type(tweet))
                author_info = user_dict[tweet.author_id]
                # check the code below!!!
                #media_info = {'type': ''} if get_attachment_key(tweet['attachments']) == '' else media_dict[get_attachment_key(tweet['attachments'])]
        # creating the dictionary from tweet and user data and media data
                result.append({'author_id': tweet.author_id,
                       'username': author_info['username'],
                       'author_followers': author_info['followers'],
                       'author_tweets': author_info['tweets'],
                       #'author_image': author_info['imageUrl'],
                       'author_description': author_info['description'],
                       'author_location': author_info['location'],
                       'tweet_id': tweet.id,
                       'text': tweet.text,
                       'created_at': tweet.created_at,
                       'retweets': tweet.public_metrics['retweet_count'],
                       'replies': tweet.public_metrics['reply_count'],
                       'likes': tweet.public_metrics['like_count'],
                       'quote_count': tweet.public_metrics['quote_count'],
                       'referenced_tweets_list': tweet.referenced_tweets,
                       #'referenced_tweet': get_ref_tweet(tweet.referenced_tweets),
                       #'referenced_tweet_type': get_ref_tweet_type(tweet.referenced_tweets),
                       'conversation_id': tweet.conversation_id,
                       'attach_list': tweet['attachments']
                      })

    df = pd.DataFrame(result)

    return df

def get_recent_tweets (query_string, token, start_time, end_time):
    """
    gets data frame of tweets based on the search string
    search string can either be hashtag or conversation id
    returns data frame of tweets
    """

    hoax_tweets = []

    client = tweepy.Client(bearer_token=token)

    for response in tweepy.Paginator(client.search_recent_tweets,
                                 query = query_string,
                                 user_fields = ['username', 'public_metrics', 'description', 'location'],
                                 tweet_fields = ['created_at', 'geo', 'public_metrics', 'text', 'conversation_id'],
                                 media_fields = ['media_key', 'type'],
                                 expansions = ['author_id', 'referenced_tweets.id', 'attachments.media_keys'],
                                 start_time = start_time,
                                 end_time = end_time,
                             max_results=100):


        time.sleep(1)
        hoax_tweets.append(response)

    result = []
    user_dict = {}
    media_dict = {}

# get responses from tweet call
    if response[0] != None:
        for response in hoax_tweets:
    # get users data from tweet call
            for user in response.includes['users']:
                user_dict[user.id] = {'username': user.username,
                              'followers': user.public_metrics['followers_count'],
                              'tweets': user.public_metrics['tweet_count'],
                              'description': user.description,
                              'location': user.location,
                              #'imageUrl': user.profile_image_url
                             }

            # get media files if exists
            try:
                for m in response.includes['media']:
                    media_dict[m.media_key] = {'type': m.type
                                            }
            except KeyError:
                pass


            for tweet in response.data:
                author_info = user_dict[tweet.author_id]
                #media_info = {'type': ''} if get_attachment_key(tweet['attachments']) == '' else media_dict[get_attachment_key(tweet['attachments'])]
        # creating the dictionary from tweet and user data and media data
                result.append({'author_id': tweet.author_id,
                       'username': author_info['username'],
                       'author_followers': author_info['followers'],
                       'author_tweets': author_info['tweets'],
                       #'author_image': author_info['imageUrl'],
                       'author_description': author_info['description'],
                       'author_location': author_info['location'],
                       'tweet_id': tweet.id,
                       'text': tweet.text,
                       'created_at': tweet.created_at,
                       'retweets': tweet.public_metrics['retweet_count'],
                       'replies': tweet.public_metrics['reply_count'],
                       'likes': tweet.public_metrics['like_count'],
                       'quote_count': tweet.public_metrics['quote_count'],
                       'referenced_tweets_list': tweet.referenced_tweets,
                       #'referenced_tweet': get_ref_tweet(tweet.referenced_tweets),
                       #'referenced_tweet_type': get_ref_tweet_type(tweet.referenced_tweets),
                       'conversation_id': tweet.conversation_id,
                       'attach_list': tweet['attachments'],
                       #'attach_len': len(tweet['attachments']),
                       #'attach_key': get_attachment_key(tweet['attachments']),
                       #'media_type': media_info['type']
                        #get_attachment_key
                      })

    df = pd.DataFrame(result)

    return df

def write_tweets_s3_bucket(df: pandas.DataFrame) -> None:
    """ Writes weekly players data from fpl api to s3

    :param ti: used for xcom pull, task_instance

    :param num_players: used to control number of players pulled to s3, for testing purpose
    """

    s3 = create_boto3(True)

    print('Copying json ply data to s3')
    num_s3_ply_data = 0

    # !!! add data flow
    # !!! changed to 15; num_players
    json_file = df.to_json(orient='records')

    time.sleep(0.2)
    s3object = s3.Object('mylosh', F'tweet/elon.json')
    s3object.put(
        Body=(bytes(json.dumps(json_file).encode('UTF-8')))
    )


    #print(F'Finished copying ply data jsons, total: {num_s3_ply_data}')


def create_boto3(resource: bool) -> boto3:

    config_obj = configparser.ConfigParser()
    config_obj.read('config.ini')
    # db_param = config_obj["postgresql"]
    aws_user = config_obj["aws"]
    # db_user = db_param['user']
    # db_pass = db_param['password']
    # db_host = db_param['host']
    # db_name = db_param['db']

    if resource == 1:
        s3 = boto3.resource(
            service_name='s3',
            region_name=aws_user['region'],
            aws_access_key_id=aws_user['acc_key'],
            aws_secret_access_key=aws_user['secret_acc_key']
        )
    else:
        s3 = boto3.client(
            service_name='s3',
            region_name=aws_user['region'],
            aws_access_key_id=aws_user['acc_key'],
            aws_secret_access_key=aws_user['secret_acc_key']
        )

    return s3