#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 1/04/2019 8:46 PM
# @Author  : Pengfei Xiao
# @FileName: restful_replies.py
# @Software: PyCharm

import sys
import pandas as pd
from tweepy import AppAuthHandler, TweepError, API
sys.path.append('..')
from analyser import functional_tools
import threading
import gc

gc.enable()

# Twitter API Keys-Siyu
CONSUMER_KEY = "wWFHsJ71LrXoX0LRFNCVYxLoY"
CONSUMER_SECRET = "dpOn4LvtZ0MqxgtFZB0XXFKz9wK7csAHLkusJ8JasUJIxFt6Qm"
ACCESS_TOKEN = "1104525213847318529-S0OLx8OztXjSxeGCGITcGhVa2EMz5b"
ACCESS_TOKEN_SECRET = "wEAjXPqWPygScOzAc8RRwiHzeg1G0mGVt20qZLoJGQuDe"


# # Twitter API Keys-yiru
# CONSUMER_KEY = '9uWwELoYRA4loNboCqe4P7XZD'
# CONSUMER_SECRET = 'ZhIOn2XPAnVtDjbh4iVrANG4gq7zTCJdJZAAlDpPmKAFpNz4gF'
# ACCESS_TOKEN = '2344719422-4a94VSU2kjHzgFp1Kap9uoAAvE5R2n9vb4H5Atz'
# ACCESS_TOKEN_SECRET = 'O5H5r7QyOTct7yFFlePITJGcuIJPBmgyDBunIYRVjYELq'


# # # # TWITTER AUTHENTICATER # # # #
class TwitterAuthenticator():
    """

    """

    def authenticate_twitter_app(self):
        """

        :return:
        """
        # auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
        # auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
        auth = AppAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
        return auth


# class RestfulCrawler(Process):
class RestfulReplies(threading.Thread):
    """

    """

    def __init__(self, screen_name, db_name, collection_name):
        """
        :param twitter_user:
        """
        # super().__init__()
        threading.Thread.__init__(self)
        self.auth = TwitterAuthenticator().authenticate_twitter_app()
        self.twitter_api = API(self.auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True, timeout=200)
        self.SCREEN_NAME = screen_name  # 'ScottMorrisonMP'
        self.db_name = db_name
        self.collection_name = collection_name

    def run(self):

        max_id = None
        NUM_PER_QUERY = 100
        records_count = 0
        f_tools = functional_tools.FunctionalTools()

        while True:
            try:
                raw_tweets = self.twitter_api.search(q='to:' + self.SCREEN_NAME, tweet_mode='extended', max_id=max_id,
                                                     count=NUM_PER_QUERY)
                if len(raw_tweets) == 0:
                    print("No more replies found.")
                    print('In total {} replies are stored in DB.'.format(records_count))
                    print('-----')
                    break

                max_id = raw_tweets[-1].id - 1  # update max_id to harvester earlier data
                df = f_tools.tweets_to_dataframe(raw_tweets)

                if df.shape[0] != 0:
                    f_tools.save_data(df.to_dict('records'), self.db_name, self.collection_name, 'update')
                    records_count += df.shape[0]

            except TweepError as e1:
                print('Restful reply error:')
                print(e1)
                break

            except Exception as e2:
                print(e2)
                break


if __name__ == "__main__":
    temp_df = pd.read_csv('../data/new_politician_list.csv', usecols=['ScreenName'])
    politician_list = temp_df['ScreenName'].dropna().tolist()
    for screen_name in politician_list[:1]:
        print('============================================')
        print('Process: {}/{}'.format(politician_list.index(screen_name) + 1, len(politician_list)))
        # restful_replies = RestfulReplies(screen_name, 'capstone', 'restfulMentioned')
        restful_replies = RestfulReplies(screen_name, 'test', 'test99')
        print("Crawling replies to {}.".format(screen_name))
        restful_replies.start()
        restful_replies.join()
        # df.to_json('Replies_Info.json', orient='records')
