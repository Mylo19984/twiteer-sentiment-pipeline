from application import app
from flask import render_template
#from application import mongo
from application import tweet


@app.route('/')
def dashboard():
    #result = mongo.db.tweet.find()

    #for r in result:
    #    print(r)

    print('kkk')
    print(tweet.find())

    result = tweet.find({}, {"text": 1, "berta_sent_analysis_score": 1}).limit(20)
    result_query = list(result)
    tweet_data_label = [row['text'] for row in result_query]
    tweet_data = [round(float(row['berta_sent_analysis_score']),2) for row in result_query]

    return render_template('dashboard.html', title='Dash', fpl_data=tweet_data, fpl_data_name=tweet_data_label)



