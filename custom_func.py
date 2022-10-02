import configparser
import pandas as pd


def read_config():
    config = configparser.ConfigParser(interpolation=None)
    config.read('config.ini')
    return config


def save(tweets: pd.DataFrame, path: str):

    try:
        tweets.to_json(path, orient='records')
    except Exception as e:
        print('Exception happened, it is', e.__class__)
    else:
        tweets.to_json(path, orient='records')

