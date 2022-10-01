from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, FloatType, LongType
from pyspark.sql.types import IntegerType, TimestampType
from pyspark.sql import functions as f
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from transformers import pipeline
import boto3
import configparser
import json
from pyspark.sql.functions import *
import re
from pymongo import MongoClient
import findspark

config_obj = configparser.ConfigParser()
config_obj.read('config.ini')
# db_param = config_obj["postgresql"]
aws_user = config_obj["aws"]

s3 = boto3.client(
     service_name='s3',
     region_name=aws_user['region'],
     aws_access_key_id=aws_user['acc_key'],
     aws_secret_access_key=aws_user['secret_acc_key']
     )

obj = s3.get_object(Bucket='mylosh', Key=F'tweet/elon.json')
j = json.loads(obj['Body'].read().decode())

findspark.init()

spark = (SparkSession
        .builder
        .appName('mylo') \
        .master('local')\
        .config("spark.driver.memory", "2g") \
        .getOrCreate())

schema_tweet = StructType([
StructField("author_id", IntegerType(),True),
StructField("username", StringType(),True),
StructField("author_followers", IntegerType(),True),
StructField("author_tweets", IntegerType(),True),
StructField("author_description", StringType(),True),
StructField("author_location", StringType(),True),
StructField("tweet_id", LongType(),True),
StructField("text", StringType(),True),
StructField("created_at", StringType(),True),
StructField("retweets", IntegerType(),True),
StructField("replies", IntegerType(),True),
StructField("likes", IntegerType(),True),
StructField("quote_count", IntegerType(),True),
StructField("conversation_id", LongType(),True),
StructField("attach_list",
            StructType(
            [
                StructField("media_keys", StringType(), True)
            ]
                        )
            ,True),
                StructField("referenced_tweets_list",
            StructType([
            StructField("data",
            StructType([
                StructField("type", StringType(), True),
                StructField("id", StringType(), True)
                ])
            , True)
            ])
            ,True)
])

df = spark.createDataFrame(j, schema=schema_tweet)

my_punctuation = '!"$%&\'()*+,-./:;<=>?[\\]^_`{|}~@'

def remove_punctuation(tweet):
    t = re.sub(r'^https?:\/\/.*[\r\n]*', '', tweet)
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


remove_links=udf(remove_links, StringType())
remove_punctuation=udf(remove_punctuation, StringType())
remove_users = udf(remove_users, StringType())
remove_hashtag = udf(remove_hashtag, StringType())

df_clean_text = df.withColumn('cleaned_tweet_text', remove_links(df['text']))
df_clean_text = df_clean_text.withColumn('cleaned_tweet_text', remove_users(df_clean_text['cleaned_tweet_text']))
df_clean_text = df_clean_text.withColumn('cleaned_tweet_text', remove_punctuation(df_clean_text['cleaned_tweet_text']))
df_clean_text = df_clean_text.withColumn('cleaned_tweet_text', remove_hashtag(df_clean_text['cleaned_tweet_text']))

model_name = 'distilbert-base-uncased-finetuned-sst-2-english'
model = AutoModelForSequenceClassification.from_pretrained(model_name)
tokenizer = AutoTokenizer.from_pretrained(model_name)
classifier = pipeline('sentiment-analysis', model=model, tokenizer=tokenizer)

model_name_berta = 'cardiffnlp/twitter-roberta-base-sentiment-latest'
model_berta = AutoModelForSequenceClassification.from_pretrained(model_name_berta)
tokenizer_berta = AutoTokenizer.from_pretrained(model_name_berta)
classifier_berta = pipeline('sentiment-analysis', model=model_berta, tokenizer=tokenizer_berta)

def berta_classifier_sentiment_lable(text):
    text = classifier_berta(text)
    return text[0]['label']

def berta_classifier_sentiment_score(text):
    text = classifier_berta(text)
    return text[0]['score']

berta_classifier_sentiment_lable = udf(berta_classifier_sentiment_lable, StringType())
berta_classifier_sentiment_score = udf(berta_classifier_sentiment_score, FloatType())

df_sentiment_label = df_clean_text.withColumn('berta_sent_analysis_label', berta_classifier_sentiment_lable(df_clean_text['cleaned_tweet_text']))
df_sentiment_label = df_sentiment_label.withColumn('berta_sent_analysis_score', berta_classifier_sentiment_score(df_clean_text['cleaned_tweet_text']))

df_date_manipul = df_sentiment_label.withColumn('created_at', (df_sentiment_label['created_at']/1000).cast(LongType()))
df_date_manipul = df_date_manipul.withColumn('created_at_date', f.from_utc_timestamp(df_date_manipul['created_at'].cast(TimestampType()), 'PST'))
df_date_manipul = df_date_manipul.withColumn('year', year(col('created_at_date'))) \
                                .withColumn('month', month(col('created_at_date'))) \
                                .withColumn('dayofmonth', dayofmonth(col('created_at_date')))

df_final = df_date_manipul.drop('created_at')

df3 = df_final.coalesce(1)
print(df3.rdd.getNumPartitions())

client = MongoClient("mongodb://localhost:27017/", username= 'rootuser', password= 'rootpass')
mylo_db = client["mylocode"]

print('converting to dict data')
df_insert_db = df3.toPandas().to_dict(orient='records')
print('converting to dict finished')

print('insert db')
mylo_db.tweet.insert_many(df_insert_db)
print('print finished insert db')

