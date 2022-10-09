from pyspark.sql.types import StructField, FloatType, LongType
from pyspark.sql.types import IntegerType, TimestampType
from pyspark.sql.functions import *
from pyspark.sql import functions as f
import re
from tweet_search import create_boto3
import json


def transformation_date(df):
    """ Transforms the date columns in dataframe

    :param df: dataframe which needs transformation of date columns

    :return: dataframe with new columns - date, year, month, day of month
    """

    df_date_manipul = df.withColumn('created_at', (col('created_at') / 1000).cast(LongType()))
    df_date_manipul = df_date_manipul.withColumn('created_at_date', f.from_utc_timestamp(col('created_at').cast(TimestampType()), 'PST'))
    df_date_manipul = df_date_manipul.withColumn('year', year(col('created_at_date'))) \
        .withColumn('month', month(col('created_at_date'))) \
        .withColumn('dayofmonth', dayofmonth(col('created_at_date')))

    df_final = df_date_manipul.drop('created_at')

    return df_final


def remove_punctuation(tweet: str) -> str:
    """ Transform tweet text, and eliminates the unnecessary characters

    :param tweet: as inputs we get text which needs clean up

    :return: cleaned tweet
    """

    my_punctuation = '!"$%&\'()*+,-./:;<=>?[\\]^_`{|}~@'
    t = re.sub('['+my_punctuation + ']+', ' ', tweet)
    return t


def remove_links(tweet):
    """ Transform tweet text, and eliminates links

    :param tweet: as inputs we get text which needs clean up

    :return: cleaned tweet
    """

    tweet = re.sub(r'http\S+', '', tweet)
    return tweet


def remove_users(tweet):
    """ Transform tweet text, and eliminates users characters

    :param tweet: as inputs we get text which is being cleand

    :return: cleaned tweet
    """

    tweet = re.sub('(RT\s@[A-Za-z]+[A-Za-z0-9-_]+)', '', tweet)
    tweet = re.sub('(@[A-Za-z]+[A-Za-z0-9-_]+)', '', tweet)
    return tweet


def remove_hashtag(tweet):
    """ Transform tweet text, and eliminates hashtags

    :param tweet: as inputs we get text which is being cleand

    :return: cleaned tweet
    """

    tweet = re.sub('(#[A-Za-z]+[A-Za-z0-9-_]+)', '', tweet)
    return tweet

def create_schema():
    """ Creates schema necessary for pyspark and further datatransformation

    :return: pyspark dataframe schema
    """

    schema_tweet = StructType([
        StructField("author_id", IntegerType(), True),
        StructField("username", StringType(), True),
        StructField("author_followers", IntegerType(), True),
        StructField("author_tweets", IntegerType(), True),
        StructField("author_description", StringType(), True),
        StructField("author_location", StringType(), True),
        StructField("tweet_id", LongType(), True),
        StructField("text", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("retweets", IntegerType(), True),
        StructField("replies", IntegerType(), True),
        StructField("likes", IntegerType(), True),
        StructField("quote_count", IntegerType(), True),
        StructField("conversation_id", LongType(), True),
        StructField("attach_list",
                    StructType(
                        [
                            StructField("media_keys", StringType(), True)
                        ]
                    )
                    , True),
        StructField("referenced_tweets_list",
                    StructType([
                        StructField("data",
                                    StructType([
                                        StructField("type", StringType(), True),
                                        StructField("id", StringType(), True)
                                    ])
                                    , True)
                    ])
                    , True)
    ])

    return schema_tweet

def pulling_json_s3():
    """ Pulls the json object from s3, and returns it as json file

    :return: json file needed for upload to pyspark dataframe
    """

    s3 = create_boto3(False)
    obj = s3.get_object(Bucket='mylosh', Key=F'tweet/elon.json')
    json_file = json.loads(obj['Body'].read().decode())

    return json_file



