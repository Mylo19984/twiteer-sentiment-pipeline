import tweepy
import pandas as pd
import boto3
import json
import configparser
import time
from pymongo import MongoClient
from datetime import datetime


def get_recent_user_tweets (query_string: str, token: str, start_time: datetime, end_time: datetime, since_id: str) -> pd.DataFrame:
    """ Pulls tweets from the specific user from twitter

    :param query_string: string of twitter user id, for whom tweets are being pulled

    :param token: token for twitter api

    :param start_time: date from which tweets are being pulled

    :param end_time: date to which tweets are being pulled

    :param since_id: id of tweet starting which tweets are being pulled

    :return: dataframe of tweets
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
                                 since_id = since_id,
                                 max_results=100):

        time.sleep(0.5)
        hoax_tweets.append(response)

    result = []
    user_dict = {}

    if hoax_tweets[0].data is not None:

        for response in hoax_tweets:
            # print(response.data)
            # get users data from tweet call
            try:
                for user in response.includes['users']:
                    user_dict[user.id] = {'username': user.username,
                                  'followers': user.public_metrics['followers_count'],
                                  'tweets': user.public_metrics['tweet_count'],
                                  'description': user.description,
                                  'location': user.location,
                                 }

                for tweet in response.data:
                    author_info = user_dict[tweet.author_id]
                    # restructure
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

            except Exception as e:
                print(F'Error in tweepy pull occurred: {e.__class__}')
                print(e)
                pass

    else:
        print('There is no data in tweepy json file')

    # refactoring
    #save_last_tweet_id(result[0]['tweet_id'])
    #print(result[0]['tweet_id'])
    df = pd.DataFrame(result)
    print(F'Number of tweets: {df.shape[0]}')

    return df


def get_recent_tweets (query_string, token, start_time, end_time) -> pd.DataFrame:
    """ Pulls tweets from the specific hashtag from twitter

    :param query_string: string of twitter hashtag, for whom tweets are being pulled

    :param token: token for twitter api

    :param start_time: date from which tweets are being pulled

    :param end_time: date to which tweets are being pulled

    :return: dataframe of tweets
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
    for response in hoax_tweets:
        # get users data from tweet call
        for user in response.includes['users']:
            user_dict[user.id] = {'username': user.username,
                          'followers': user.public_metrics['followers_count'],
                          'tweets': user.public_metrics['tweet_count'],
                          'description': user.description,
                          'location': user.location,
                         }

        for tweet in response.data:
            author_info = user_dict[tweet.author_id]
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


def write_tweets_s3_bucket(df: pd.DataFrame, file_name: str) -> None:
    """ Writes tweet data from twitter to s3, in json format

    :param df: DataFrame which is forwarded from function which pulls data

    :param file_name: String in which name of the file is being kept
    """

    if df.shape[0] > 0:

        try:
            s3 = create_boto3(True)

            print('Copying json data to s3')

            if int(get_last_tweet_id_s3()) < df.iloc[0]['tweet_id']:

                save_last_tweet_id_s3(df.iloc[0]['tweet_id'])
                json_file = df.to_json(orient='records')
                s3object = s3.Object('mylosh', F'tweet/{file_name}.json')
                s3object.put(
                    Body=(bytes(json_file.encode('UTF-8'))), ContentType='application/json'
                )

                print('Finished copying json data')

            else:
                print('json files is already on s3')

        except Exception as e:
            print(F'Error in s3 saving occurred: {e.__class__}')
            print(e)

    else:
        print('Json file is empty, nothing to insert in s3')


def write_tweets_s3_mongodb() -> None:
    """ Writes twitter data from s3 to mongodb

    """

    config_obj = read_config()
    db_param = config_obj["mongoDb"]
    db_pass = db_param['pass']
    db_host = db_param['host']
    db_user = db_param['user']
    s3 = create_boto3(False)
    data_bucket = s3.list_objects(Bucket='mylosh', Prefix='tweet/')['Contents']
    # getting the last modified date file on s3
    list_s3_obj = [obj['Key'] for obj in sorted(data_bucket, key=lambda obj: int(obj['LastModified'].strftime('%s')), reverse=True)]
    obj = s3.get_object(Bucket='mylosh', Key=list_s3_obj[0])
    j = json.loads(obj['Body'].read().decode())

    print('Lastes tweet id in mongo ' + get_last_tweet_id_mongo())
    print('Latest tweet id in json file ' + str(j[0]['tweet_id']))

    if j[0]['tweet_id']>int(get_last_tweet_id_mongo()):

        print('mongo db part start')

        client = MongoClient(F"{db_host}", username=F'{db_user}', password=F'{db_pass}')
        mylo_db = client["mylocode"]

        try:
            mylo_db.tweet_raw.insert_many(j)
            print('Saving insert log')
            save_last_tweet_id_db(j[0]['tweet_id'])
            print(F'Finished insert log, number of records: {len(j)}')
        except Exception as e:
            print('Exception happened in mongodb insert, it is', e.__class__)
            print(e)

        print('mongo db part end')

    else:
        print('all data is already in the db')


def create_boto3(resource: bool) -> boto3:
    """ Creates boto3 object

    :param resource: boolean indicator if resource or client is being created

    :return: boto3 boject for further use of connecting to s3
    """

    config_obj = read_config()
    aws_user = config_obj["aws"]

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

# other functions


def read_config() -> configparser:
    """ Reads config.ini file

    :return: configparser object with config.ini data
    """

    config = configparser.ConfigParser(interpolation=None)
    config.read('config.ini')

    return config


def save(tweets: pd.DataFrame, path: str):
    """ Takes dataframe with tweets and saves it as json object in root directory for testing purpose

    :param tweets: dataframe object which is being coverted to json

    :param path: path where file should b saved nad how it should be named
    """

    try:
        tweets.to_json(path, orient='records')
    except Exception as e:
        print('Exception happened in saving file to disk, it is', e.__class__)
        print(e)
    else:
        tweets.to_json(path, orient='records')


def save_last_tweet_id_s3(id: str):
    """

    """

    now_date = datetime.now()

    dictionary_tweet_id = {
        "id": str(id),
        "date_time": now_date
    }

    s3 = create_boto3(True)

    print('Copying last tweet id to s3')

    json_file = json.dumps(dictionary_tweet_id, default=str)

    s3object = s3.Object('mylosh', F'tweet_id/tweet_id.json')
    s3object.put(
        Body=(bytes(json_file.encode('UTF-8'))), ContentType='application/json'
    )

    print('Finished last tweet id')


def get_last_tweet_id_s3() -> str:
    """

    """

    s3 = create_boto3(False)

    obj = s3.get_object(Bucket='mylosh', Key=F'tweet_id/tweet_id.json')
    j = json.loads(obj['Body'].read().decode())
    j_id = j['id']

    return str(j_id)


def save_last_tweet_id_db(id):
    """

    """

    now_date = datetime.now()
    dictionary_tweet_id = {
        "id": str(id),
        "date_time": now_date
    }
    config_obj = read_config()
    db_param = config_obj["mongoDb"]
    db_pass = db_param['pass']
    db_host = db_param['host']
    db_user = db_param['user']

    client = MongoClient(F"{db_host}", username=F'{db_user}', password=F'{db_pass}')
    mylo_db = client["mylocode"]

    try:
        mylo_db.insert_log.insert_one(dictionary_tweet_id)
    except Exception as e:
        print('Exception happened in saving last id in mongo, it is', e.__class__)
        print(e)


def get_last_tweet_id_mongo() -> str:
    """

    """

    config_obj = read_config()
    db_param = config_obj["mongoDb"]
    db_pass = db_param['pass']
    db_host = db_param['host']
    db_user = db_param['user']

    client = MongoClient(F"{db_host}", username=F'{db_user}', password=F'{db_pass}')
    mylo_db = client["mylocode"]

    json_data = mylo_db.insert_log.find({}, {"date_time":1, "id":1}).sort("date_time",-1).limit(1)

    return str(json_data[0]['id'])