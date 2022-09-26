import tweepy
#from twitter_authentication import bearer_token
import time
import pandas as pd
import numpy as np
import re

def get_ref_tweet(lis):
    """"
    gets referenced tweet id from tweets list
    returns referenced tweet id
    """

    if type(lis) == type(None):
        pass
    else:
        return re.search(r'(?<==)(.*?)(?= )',str(lis[0])).group(0)


def get_ref_tweet_type(lis):
    """"
    gets referenced tweet type from tweets list
    returns referenced tweet type
    """

    if type(lis) == type(None):
        pass
    else:
        return re.search(r'(?<=type=).*',str(lis[0])).group(0)

def get_attachment_key(lis):
    """"
    gets attachemt objet of the tweet
    returns attachment object for next manipulation
    """

    if type(lis) == type(None):
        return ''
    else:
        return lis['media_keys'][0]

def get_conversation_id_for_search(df):
    """"
    gets the conversation if of data frame object which has list of tweets
    returns conversation ids of data frame
    """
    df_conversation = df[df['replies']>0]
    lis = df_conversation['conversation_id']
    print(type(lis.tolist()))
    return lis.tolist()


def get_recent_tweets (query_string, token, start_time, end_time):
    """
    gets data frame of tweets based on the search string
    search string can either be hashtag or conversation id
    returns data frame of tweets
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
    if response[0] != None:
        for response in hoax_tweets:
    # get users data from tweet call
            for user in response.includes['users']:
                user_dict[user.id] = {'username': user.username,
                              'followers': user.public_metrics['followers_count'],
                              'tweets': user.public_metrics['tweet_count'],
                              'description': user.description,
                              'location': user.location,
                              #'imageUrl': user.profile_image_url
                             }

            # get media files if exists
            try:
                for m in response.includes['media']:
                    media_dict[m.media_key] = {'type': m.type
                                            }
            except KeyError:
                pass


            for tweet in response.data:
                author_info = user_dict[tweet.author_id]
                media_info = {'type': ''} if get_attachment_key(tweet['attachments']) == '' else media_dict[get_attachment_key(tweet['attachments'])]
        # creating the dictionary from tweet and user data and media data
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
                       'referenced_tweet': get_ref_tweet(tweet.referenced_tweets),
                       'referenced_tweet_type': get_ref_tweet_type(tweet.referenced_tweets),
                       'conversation_id': tweet.conversation_id,
                       'attach_list': tweet['attachments'],
                       #'attach_len': len(tweet['attachments']),
                       'attach_key': get_attachment_key(tweet['attachments']),
                       'media_type': media_info['type']
                        #get_attachment_key
                      })

    df = pd.DataFrame(result)

    return df


def get_tweet_likes(query_string, token, start_time, end_time):
    """
    get the list of tweets that liked specific tweet_id
    returns the data frame of tweets which liked specific tweet
    """

    hoax_tweets = []

    client = tweepy.Client(bearer_token=token)

    for response in tweepy.Paginator(client.get_liking_users,
                                     id=query_string,
                                     user_fields=['username', 'public_metrics', 'description', 'location'],
                                     tweet_fields=['created_at', 'geo', 'public_metrics', 'text', 'conversation_id'],
                                     #media_fields=['media_key', 'type'],
                                     #expansions=['author_id'],
                                     #start_time=start_time,
                                     # end_time = end_time,
                                     max_results=100):
        time.sleep(1)
        hoax_tweets.append(response)

    result = []

    # get responses from tweet call
    for response in hoax_tweets:

        if type(response.data) != type(None):
            for tweet in response.data:
                print(tweet)
                # creating the dictionary from tweet and user data and media data
                result.append({'author_id':tweet['id']
                               ,'tweet_id':query_string})

    df = pd.DataFrame(result)

    return df
