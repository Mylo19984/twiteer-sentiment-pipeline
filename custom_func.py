import configparser
import pandas as pd


def read_config() -> configparser:
    """ Reads config.ini file

    :return: configparser object with config.ini data
    """
    config = configparser.ConfigParser(interpolation=None)
    config.read('config.ini')
    return config


def save(tweets: pd.DataFrame, path: str):
    """ Takes dataframe with tweets and saves it as json object in root directory for testing purpose

    :param tweets: dataframe object which is being coverted to json

    :param path: path where file should b saved nad how it should be named
    """

    try:
        tweets.to_json(path, orient='records')
    except Exception as e:
        print('Exception happened, it is', e.__class__)
    else:
        tweets.to_json(path, orient='records')

