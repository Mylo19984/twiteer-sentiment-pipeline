from pyspark.sql.types import StructField, FloatType, LongType
from pyspark.sql.types import IntegerType, TimestampType
from pyspark.sql.functions import *
from pyspark.sql import functions as f
import re
from tweet_search import create_boto3, read_config, get_last_tweet_id_mongo
import json
from pyspark.sql import SparkSession
from datetime import datetime
from pymongo import MongoClient


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
                    ArrayType(StructType([
                        StructField("data",
                                    StructType([
                                        StructField("type", StringType(), True),
                                        StructField("id", StringType(), True)
                                    ])
                                    , True)
                    ]))
                    , True)
    ])

    return schema_tweet


def pulling_json_s3_for_spark(auth_id: str):
    """ Pulls the json object from s3, and returns it as json file

    :return: json file needed for upload to pyspark dataframe
    :return: number of files needed to be inserted in mongoDb
    :return: highest modified date of filtered files
    """

    table_name = 'insert_processed_log'
    # exiting code without the data in loger
    last_modified_date = get_last_tweet_id_mongo(table_name, auth_id)
    last_modified_date_int = datetime.timestamp(datetime.strptime(last_modified_date, "%Y-%m-%d %H:%M:%S"))
    #last_modified_date_int = 0
    s3 = create_boto3(True)
    bucket = s3.Bucket('mylosh')

    no_of_files = 0
    final_list = []
    #last_modified_date = 0
    table_name = 'insert_processed_log'

    for obj in bucket.objects.filter(Prefix=F'tweet/id_{auth_id}'):

        if (obj.get()['ContentLength'] > 0) and (int(obj.get()['LastModified'].strftime('%s')) > last_modified_date_int):
            body = json.load(obj.get()['Body'])
            final_list.append(body)
            no_of_files += 1
            last_modified_date_int = int(obj.get()['LastModified'].strftime('%s'))

    modified_date_marker = datetime.fromtimestamp(last_modified_date_int, tz = None)

    return final_list, no_of_files, modified_date_marker


def pulling_json_s3_for_spark_v5():
    """ Pulls the json object from s3, and returns it as json file

    :return: json file needed for upload to pyspark dataframe
    """

    spark = (SparkSession
             .builder
             .appName('mylo')
             .master('local')
             .config("spark.driver.memory", "2g")
             .getOrCreate())

    config_obj = read_config()
    aws_user = config_obj["aws"]

    spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", aws_user['acc_key'])
    spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", aws_user['secret_acc_key'])
    spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")

    schema_tweet = create_schema()

    df = spark.read.json("s3://mylosh/tweet/id_44196397_1665511144.828783.json")
    #data/id_44196397_1665511144.828783.json

        # spark.createDataFrame(json_file, schema=schema_tweet)

    print(df.head())

    return df


def save_last_tweet_id_db(file_modified_date, author_id):
    """

    """

    now_date = datetime.now()
    dictionary_db_logger = {
        "file_modified_date": file_modified_date,
        "author_id": author_id,
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
        mylo_db.insert_processed_log.insert_one(dictionary_db_logger)
    except Exception as e:
        print('Exception happened in saving last id in mongo, it is', e.__class__)
        print(e)



