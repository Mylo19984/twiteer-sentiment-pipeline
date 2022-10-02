from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, FloatType, LongType
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from transformers import pipeline
import configparser
import json
from pyspark.sql.functions import *
from pymongo import MongoClient
from tweet_search import create_boto3
from pyspark_transformation_func import remove_punctuation, remove_users, remove_links, remove_hashtag
from pyspark_transformation_func import transformation_date, create_schema


config_obj = configparser.ConfigParser()
config_obj.read('config.ini')
# db_param = config_obj["postgresql"]
aws_user = config_obj["aws"]


def berta_classifier_sentiment_lable(text):

    text = classifier_berta(text)
    return text[0]['label']


def berta_classifier_sentiment_score(text):

    text = classifier_berta(text)
    return text[0]['score']


def clean_tweet_text(df):

    df_clean_text = df.withColumn('cleaned_tweet_text', remove_links(col('text')))
    df_clean_text = df_clean_text.withColumn('cleaned_tweet_text', remove_users(col('cleaned_tweet_text')))
    df_clean_text = df_clean_text.withColumn('cleaned_tweet_text', remove_punctuation(col('cleaned_tweet_text')))
    df_clean_text = df_clean_text.withColumn('cleaned_tweet_text', remove_hashtag(col('cleaned_tweet_text')))

    return df_clean_text


def add_sentiment_columns(df):
    df_sentiment_label = df.withColumn('berta_sent_analysis_label',
                                        berta_classifier_sentiment_lable(col('cleaned_tweet_text')))
    df_sentiment_label = df_sentiment_label.withColumn('berta_sent_analysis_score',
                                        berta_classifier_sentiment_score(col('cleaned_tweet_text')))

    return df_sentiment_label


def pulling_json_s3():
    s3 = create_boto3(False)
    obj = s3.get_object(Bucket='mylosh', Key=F'tweet/elon.json')
    json_file = json.loads(obj['Body'].read().decode())

    return json_file


# creation of connection to s3, and getting the json file
json_file = pulling_json_s3()

# inserting json file into the spark dataframe
spark = (SparkSession
        .builder
        .appName('mylo')
        .master('local')
        .config("spark.driver.memory", "2g")
        .getOrCreate())

schema_tweet = create_schema()

df = spark.createDataFrame(json_file, schema=schema_tweet)

# cleaning the tweet text with udf functions
remove_links = udf(remove_links, StringType())
remove_punctuation = udf(remove_punctuation, StringType())
remove_users = udf(remove_users, StringType())
remove_hashtag = udf(remove_hashtag, StringType())

df_clean_text = clean_tweet_text(df)

# creating the sentiment analysis columns with hugging face
model_name_berta = 'cardiffnlp/twitter-roberta-base-sentiment-latest'
model_berta = AutoModelForSequenceClassification.from_pretrained(model_name_berta)
tokenizer_berta = AutoTokenizer.from_pretrained(model_name_berta)
classifier_berta = pipeline('sentiment-analysis', model=model_berta, tokenizer=tokenizer_berta)

berta_classifier_sentiment_lable = udf(berta_classifier_sentiment_lable, StringType())
berta_classifier_sentiment_score = udf(berta_classifier_sentiment_score, FloatType())

df_sentiment_label = add_sentiment_columns(df_clean_text)

# creating dates columns
df_final = transformation_date(df_sentiment_label)

# the coalasce is set up becauese of my memory limits on mac mini computer. This part of code is note necessary
df3 = df_final.coalesce(1)

# converting the dataframe to pandas and dict, thus inserting it into the db
client = MongoClient("mongodb://localhost:27017/", username= 'rootuser', password= 'rootpass')
mylo_db = client["mylocode"]

print('converting data to dict')
df_insert_db = df3.toPandas().to_dict(orient='records')
print('converting data to dict finished')

print('insert db')
mylo_db.tweet.insert_many(df_insert_db)
print('finished insert db')

