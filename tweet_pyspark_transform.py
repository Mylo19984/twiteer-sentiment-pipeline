from pyspark.sql import SparkSession
import configparser
from pyspark.sql.functions import *
from pyspark.sql.types import StructField, FloatType, LongType
from pymongo import MongoClient
from pyspark_func import remove_punctuation, remove_users, remove_links, remove_hashtag, save_last_tweet_id_db
from pyspark_func import transformation_date, create_schema, pulling_json_s3_for_spark
from tweet_search import read_config
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from transformers import pipeline


config_obj = configparser.ConfigParser()
config_obj.read('config.ini')
# db_param = config_obj["postgresql"]
aws_user = config_obj["aws"]


def berta_classifier_sentiment_lable(text):
    """ Creates label for tweet text; it can be negative, neutral or positive

    :param text: the tweet text which should be analysed

    :return: sentiment label of the tweet
    """

    text = classifier_berta(text)
    return text[0]['label']


def berta_classifier_sentiment_score(text):
    """ Creates score for tweet text

    :param text: the tweet text which should be analysed

    :return: sentiment score of the tweet
    """

    text = classifier_berta(text)
    return text[0]['score']


def clean_tweet_text(df):
    """ Creates new text column for further use in transformation process

    :param df: dataframe with tweet data which should be transformed

    :return: dataframe with new column cleaned_tweet_text
    """

    df_clean_text = df.withColumn('cleaned_tweet_text', remove_links(col('text')))
    df_clean_text = df_clean_text.withColumn('cleaned_tweet_text', remove_users(col('cleaned_tweet_text')))
    df_clean_text = df_clean_text.withColumn('cleaned_tweet_text', remove_punctuation(col('cleaned_tweet_text')))
    df_clean_text = df_clean_text.withColumn('cleaned_tweet_text', remove_hashtag(col('cleaned_tweet_text')))

    return df_clean_text


def add_sentiment_columns(df):
    """ Creates 2 new columns: snetiment analysis label and sentiment analysis score

    :param df: dataframe which should be transformed

    :return: dataframe with new columns - berta_sent_analysis_label and berta_sent_analysis_score
    """

    df_sentiment_label = df.withColumn('berta_sent_analysis_label',
                                        berta_classifier_sentiment_lable(col('cleaned_tweet_text')))
    df_sentiment_label = df_sentiment_label.withColumn('berta_sent_analysis_score',
                                        berta_classifier_sentiment_score(col('cleaned_tweet_text')))

    return df_sentiment_label


# pulling db parameters from config file
config_obj = read_config()
db_param = config_obj["mongoDb"]
db_pass = db_param['pass']
db_host = db_param['host']
db_user = db_param['user']

# creation of connection to s3, and getting the json file
json_list, no_of_items, modified_date_marker = pulling_json_s3_for_spark()

# inserting json file into the spark dataframe
spark = (SparkSession
        .builder
        .appName('mylo')
        .master('local')
        .config("spark.driver.memory", "2g")
        .getOrCreate())

schema_tweet = create_schema()

emp_RDD = spark.sparkContext.emptyRDD()
df_all_json = spark.createDataFrame(data=emp_RDD, schema=schema_tweet)

for i in range(0, no_of_items):
    df_json = spark.createDataFrame(json_list[i], schema=schema_tweet)
    df_all_json = df_all_json.union(df_json)

# cleaning the tweet text with udf functions
remove_links = udf(remove_links, StringType())
remove_punctuation = udf(remove_punctuation, StringType())
remove_users = udf(remove_users, StringType())
remove_hashtag = udf(remove_hashtag, StringType())


df_clean_text = clean_tweet_text(df_all_json)

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

# coalesce is set up because of my memory limits on mac mini computer. This part of code is note necessary
df3 = df_final.coalesce(1)

# converting the dataframe to pandas and dict, thus inserting it into the db
client = MongoClient(F"{db_host}", username=F'{db_user}', password=F'{db_pass}')
mylo_db = client["mylocode"]

print('converting data to dict')
df_insert_db = df3.toPandas().to_dict(orient='records')
print('converting data to dict finished')

print('insert db')
try:
    mylo_db.tweet.insert_many(df_insert_db)
    # insert modified date of last s3 file;
    save_last_tweet_id_db(modified_date_marker)
except Exception as e:
    print('Exception happened in inserting final data to mongoDb, it is', e.__class__)
    print(e)

print(F'finished insert db, total number {len(df_insert_db)}')

