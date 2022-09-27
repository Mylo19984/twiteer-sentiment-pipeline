import configparser
import pandas as pd

def read_config():
    config = configparser.ConfigParser(interpolation=None)
    config.read('config.ini')
    return config

def save(tweets: pd.DataFrame, path: str):
    tweets.to_json(path, orient='records')
