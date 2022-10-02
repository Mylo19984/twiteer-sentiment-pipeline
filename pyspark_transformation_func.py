from pyspark.sql.types import StructField, FloatType, LongType
from pyspark.sql.types import IntegerType, TimestampType
from pyspark.sql.functions import *
from pyspark.sql import functions as f
import re


def transformation_date(df):
    df_date_manipul = df.withColumn('created_at', (col('created_at') / 1000).cast(LongType()))
    df_date_manipul = df_date_manipul.withColumn('created_at_date', f.from_utc_timestamp(col('created_at').cast(TimestampType()), 'PST'))
    df_date_manipul = df_date_manipul.withColumn('year', year(col('created_at_date'))) \
        .withColumn('month', month(col('created_at_date'))) \
        .withColumn('dayofmonth', dayofmonth(col('created_at_date')))

    df_final = df_date_manipul.drop('created_at')

    return df_final


def remove_punctuation(tweet):
    my_punctuation = '!"$%&\'()*+,-./:;<=>?[\\]^_`{|}~@'
    t = re.sub('['+my_punctuation + ']+', ' ', tweet)
    return t


def remove_links(tweet):
    tweet = re.sub(r'http\S+', '', tweet)
    return tweet


def remove_users(tweet):
    tweet = re.sub('(RT\s@[A-Za-z]+[A-Za-z0-9-_]+)', '', tweet)
    tweet = re.sub('(@[A-Za-z]+[A-Za-z0-9-_]+)', '', tweet)
    return tweet


def remove_hashtag(tweet):
    tweet = re.sub('(#[A-Za-z]+[A-Za-z0-9-_]+)', '', tweet)
    return tweet

def create_schema():
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



