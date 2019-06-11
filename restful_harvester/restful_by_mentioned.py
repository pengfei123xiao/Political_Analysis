#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 13/04/2019 2:15 PM
# @Author  : Pengfei Xiao
# @FileName: restful_by_mentioned.py
# @Software: PyCharm

"""This file is used to crawl mention tweets."""

import sys
import pandas as pd
from tweepy import AppAuthHandler, API, TweepError

sys.path.append('..')
from analyser import functional_tools
import threading
import gc

gc.enable()

# Twitter API Keys-PF
ACCESS_TOKEN = "967540920290754560-IWECpltxXhsZGUCQLPAGH1xIXds2TEz"
ACCESS_TOKEN_SECRET = "l96uRBEq4s2syHCCvmtGBooe1XqsMZT7Jo2R0D1u9WeqO"
CONSUMER_KEY = "YZXvTb6qbwRuPflCS6gUDuxnL"
CONSUMER_SECRET = "uZi7W0uUEfCGi4vwlkruX0JSe8JZV47UH5AFHptU0JYdPpJTZQ"


class RestfulByMentioned(threading.Thread):
    def __init__(self, screen_name, db_name, collection_name):
        threading.Thread.__init__(self)
        self.auth = functional_tools.FunctionalTools().authenticate_twitter_app(CONSUMER_KEY, CONSUMER_SECRET)
        self.twitter_api = API(self.auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True, timeout=200)
        self.SCREEN_NAME = screen_name
        self.db_name = db_name
        self.collection_name = collection_name

    def run(self):
        max_id = None
        TWEETS_PER_QUERY = 100
        records_count = 0
        f_tools = functional_tools.FunctionalTools()

        while True:
            try:
                raw_tweets = self.twitter_api.search(q='@' + self.SCREEN_NAME,  # geocode="-33.854,151.216,180.00km",
                                                     tweet_mode='extended', count=TWEETS_PER_QUERY, max_id=max_id)
                if not raw_tweets:
                    print("No more mentioned tweets found.")
                    print('In total {} tweets are stored in DB.'.format(records_count))
                    print('-----')
                    break

                max_id = raw_tweets[-1].id - 1  # update max_id to harvester earlier data
                df = f_tools.tweets_to_dataframe(raw_tweets)

                if df.shape[0] != 0:
                    records_count += df.shape[0]
                    f_tools.save_data(df.to_dict('records'), self.db_name, self.collection_name, 'update')

            except TweepError as e1:
                print('Restful by mentioned error:')
                print(e1)
                break

            except Exception as e2:
                print(e2)
                break


""" Testing """
if __name__ == "__main__":
    temp_df = pd.read_csv('../data/full_politician_list.csv', usecols=['ScreenName'])
    politician_list = temp_df['ScreenName'].dropna().tolist()
    for screen_name in politician_list[:1]:
        print('============================================')
        print('Process: {}/{}'.format(politician_list.index(screen_name) + 1, len(politician_list)))
        # restful_mentioned = RestfulByMentioned(screen_name, 'capstone', 'restfulMentioned')
        restful_mentioned = RestfulByMentioned(screen_name, 'test', 'test99')
        print("Crawling tweets mentioned {}.".format(screen_name))
        restful_mentioned.start()
        restful_mentioned.join()
