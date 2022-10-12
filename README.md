# Twitter sentiment analysis

Making the pipeline which will pull twitter data and do the sentiment analysis on tweets.

#### The project is consisted of:
- python connection (library tweepy) to twitter tweets
- s3 as the place for saving json files
- pyspark used for the data transformations
- mongoDb used for storing data
- presentation layer is being done in flask and chart.js (this is in the process of development)

### The flow of the data

First step in the process is that python with tweepy connects to the twitter api, and get the tweets from the focus user. This tweets, in json format, are being saved on s3. Since this data is in raw format, the transformation of it is being done on pyspark (cleaning the test in tweets from unnecessary data, doing the sentiment analysis and adding the date fileds). In this process the sentiment analysis is being done through the hugging face and the model: "cardiffnlp/twitter-roberta-base-sentiment-latest". The last step of the process is inserting data in the mongoDb (tweet table).

This process has the control of highest tweet_id pulled from tweepy, uploaded in s3, and inserted in mongoDb; thus duplicated data is not inserted in process, and also the process is optimised not to do duplicated actions.

### Files in the project

- main.py; used to run connection to twitter api and transfers files to s3 and inserts it in mongodb.
- tweet_search.py; consists of all functions used in main.py: pulling tweets, write to s3 bucket, write to mongodb.
- tweet_pyspark_transform.py; the main python file for pyspark transformation of the data.
- pyspark_transformation_func.py; all functions used for pyspark are in this file.



