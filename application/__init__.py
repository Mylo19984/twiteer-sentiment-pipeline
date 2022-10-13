from flask import Flask
from tweet_search import read_config
from flask_pymongo import PyMongo
from pymongo import MongoClient


config_obj = read_config()
db_param = config_obj["mongoDb"]
db_pass = db_param['pass']
db_host = db_param['host_flask']
db_user = db_param['user']

app = Flask(__name__)

client = MongoClient(F"{db_host}", username=F'{db_user}', password=F'{db_pass}')
mylo_db = client["mylocode"]

tweet = mylo_db.tweet


#SECRET_KEY = os.urandom(32)
#app.config['MONGO_URI'] = F"mongodb://{db_user}:{db_pass}@{db_host}/mylocode"
#mongo = PyMongo(app)

from application import routes



