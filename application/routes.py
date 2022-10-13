from application import app
from flask import render_template
from application import tweet
import datetime


@app.route('/')
def dashboard():

    base = datetime.datetime.today()
    date_list = [base - datetime.timedelta(days=x) for x in range(5)]

    pipeline = [{ "$match" : { "dayofmonth" : { "$in": [10, 9, 8, 7, 6, 5, 4, 3] } }
                 }, {"$group": {"_id": "$dayofmonth", "Total count": {"$count": {}}, "Avg sentiment": {"$avg": "$berta_sent_analysis_score"}}
                 }, { "$sort" : {"_id": 1}}]
    result_m = tweet.aggregate(pipeline)
    result_l = list(result_m)

    result_main_table = tweet.find({}, {"cleaned_tweet_text": 1, "berta_sent_analysis_score": 1, "username": 1, "retweets": 1,
                                        "replies": 1, "likes":1, "berta_sent_analysis_label":1, "created_at_date": 1}).sort("created_at_date",-1).limit(20)
    result_main_list = list(result_main_table)

    tweet_data_label = [row['_id'] for row in result_l]
    tweet_data = [round(float(row['Avg sentiment']),2) for row in result_l]
    tweet_count = [round(float(row['Total count']),2) for row in result_l]
    tweet_main_table = [row for row in result_main_list]


    return render_template('dashboard.html', title='Dash', tweet_data=tweet_data, tweet_data_name=tweet_data_label, main_table= tweet_main_table
                           ,tweet_count_graph=tweet_count)



