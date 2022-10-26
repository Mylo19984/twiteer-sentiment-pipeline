# Twitter sentiment analysis

Making the pipeline which will pull twitter data and do the sentiment analysis on tweets. Then it will map tweet with stock or crypto coin and send the automatic email to the user with tweet and graph of price history, thus user can check if maybe the stock/crypto is worth buying.

#### The project is consisted of:
- python connection (library tweepy) to tweets
- s3 as the place for saving json files
- pyspark used for the data transformations
- mongoDb used for storing data (run on docker)
- presentation layer is being done in flask and chart.js

### The flow of the data

First step in the process is that python with tweepy connects to the twitter api, and get the tweets from the focus user. This tweets, in json format, are being saved on s3. Since this data is in raw format, the transformation of it is being done on pyspark (cleaning the text in tweets from unnecessary data, doing the sentiment analysis and adding the date fields). In this process the sentiment analysis is being done through the hugging face and the model: "cardiffnlp/twitter-roberta-base-sentiment-latest". The last step of the process is inserting data in the mongoDb (tweet table).

This process has the control of highest tweet_id pulled from tweepy, uploaded in s3, and inserted in mongoDb; thus duplicated data is not inserted in process, and also the process is optimised not to do duplicated actions.

### Files in the project

- main.py; used to run connection to twitter api and transfers files to s3 and inserts it in mongodb.
- tweet_search.py; consists of all functions used in main.py: pulling tweets, write to s3 bucket, write to mongodb.
- tweet_pyspark_transform.py; the main python file for pyspark transformation of the data.
- pyspark_transformation_func.py; all functions used for pyspark are in this file.
- flask and html files

### Mongo DB document structure

{
    _id: ObjectId("634adcd96fa6d26a4f6c5c0f"),
    author_id: 44196397,
    username: 'elonmusk',
    author_followers: 108533290,
    author_tweets: 19578,
    author_description: '',
    author_location: null,
    tweet_id: Long("1579100345472782337"),
    text: 'Because it runs recursively',
    retweets: 1560,
    replies: 2805,
    likes: 35397,
    quote_count: 131,
    conversation_id: Long("1579099265401778176"),
    attach_list: null,
    referenced_tweets_list: [ [ 'replied_to', '1579099265401778176' ] ],
    cleaned_tweet_text: 'Because it runs recursively',
    berta_sent_analysis_label: 'Neutral',
    berta_sent_analysis_score: 0.7670166492462158,
    created_at_date: ISODate("2022-10-09T08:23:55.000Z"),
    year: 2022,
    month: 10,
    dayofmonth: 9
  }

#### Data
author_id - twitter id of author
username - teeter username
author_followers - number of author followers when tweet was pulled with tweepy
author_tweets - number of author tweets when tweet was pulled with tweepy
author_description - description on users twitter profile
author_location - location on users twitter profile
tweet_id - id of tweet
text - the text of tweet, the most important part
retweets - number of tweets retweets when tweet was pulled with tweepy
replies - number of tweets replies when tweet was pulled with tweepy
likes - number of tweets likes when tweet was pulled with tweepy
quote_count - number of tweets quoting when tweet was pulled with tweepy
conversation_id - id of the conversation which tweet is being part
attach_list - list of tweet attachments
referenced_tweet_list - list of tweet on which this tweet is referenced
cleaned_tweet_text - tweet text after pyspark cleaning
berta_sent_analysis_label - sentimental label of the tweet
berta_sent_analysis_score - sentimental score of the tweet
created_at_date -  tweets created date
year - tweets year
month - tweets month
dayofmonth - tweets dayofmonth

### Website look

<img src="/images/main-page.png" alt="photo of main page view" title="Twitter Sentiment Analysis">

### Development phase

- The python code will map tweet with stock or crypto coin and send the automatic email to the user with tweet and graph of price history, thus user can check if maybe the stock/crypto is worth buying.
