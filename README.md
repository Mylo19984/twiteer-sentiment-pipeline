# Twitter sentiment analysis

Making the pipeline which will pull twitter data and do the sentiment analysis on tweets.

#### The project is consisted of:
- python connection (library tweepy) to twitter tweets
- s3 as the place for saving json files
- pyspark used for the data transformations
- mongoDb used for storing data
- presentation layer is being done in flask and chart.js (this is in the process of development)

### The flow of the data
First step in the process is that python with tweepy connects to the twitter api, and get the tweets which are in the focus. This tweets, in json format, are being saved on s3. Since this data is in raw format, the transformation of it is being done on pyspark (cleaning the test in tweets from unnecessary data, doing the sentiment analysis and adding the date fileds). In this process of transformation the sentiment analysis is being done through the hugging face and the model: "cardiffnlp/twitter-roberta-base-sentiment-latest". The last step of the process is inserting data in the mongoDb (tweet table).




