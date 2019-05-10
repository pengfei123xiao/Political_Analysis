#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 30/04/2019 3:59 PM
# @Author  : Pengfei Xiao
# @FileName: restful_pol_info.py
# @Software: PyCharm

from tweepy import OAuthHandler, AppAuthHandler, API, TweepError
import pandas as pd
import numpy as np
from functools import reduce
from pymongo import MongoClient, UpdateOne
from analyser.tweet_analyser import TweetAnalyser
from multiprocessing import Process
import threading
import datetime
import time
import gc

gc.enable()

# Twitter API Keys-yiru1
ACCESS_TOKEN = "987908747375726593-yp9g2tc5FJMfr54eIc8mI8mkboX9VNC"
ACCESS_TOKEN_SECRET = "hH5K46mLTHk5Yn51gesTnrq3YYUN5vGOBDjWx8PdBbdEm"
CONSUMER_KEY = "1RcIwPa92JvKPegrsRzSWzXht"
CONSUMER_SECRET = "AFceGbD4TzRGmqRz8oq65ovHIsAuG7WlwkzfsqEvz5WgYJDoUw"


# # # # TWITTER AUTHENTICATER # # # #
class TwitterAuthenticator():
    def authenticate_twitter_app(self):
        """

        :return:
        """
        # auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
        # auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
        auth = AppAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
        return auth


class RestfulUserInfo(threading.Thread):
    """

    """

    def __init__(self, screen_name, state_name, electorate_name, party_name, db_name, collection_name):
        """
        :param twitter_user:
        """
        # super().__init__()
        threading.Thread.__init__(self)
        self.auth = TwitterAuthenticator().authenticate_twitter_app()
        self.twitter_api = API(self.auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True, timeout=200)
        self.SCREEN_NAME = screen_name
        self.state_name = state_name
        self.electorate_name = electorate_name
        self.party_name = party_name
        self.db_name = db_name
        self.collection_name = collection_name

    def run(self):
        try:
            user_info = self.twitter_api.get_user(self.SCREEN_NAME)
            if not user_info:
                print("No user information found.")
                print('-----')

            df = TweetAnalyser().politician_info_to_dataframe(user_info, self.state_name, self.electorate_name,
                                                              self.party_name)

            if df.shape[0] != 0:
                TweetAnalyser().save_data(df.to_dict('records'), self.db_name, self.collection_name, 'update')

        except TweepError as e1:
            print('Restful by user info error:')
            print(e1)

        except Exception as e2:
            print(e2)


if __name__ == "__main__":
    temp_df = pd.read_csv('../data/full_politician_list.csv',
                          usecols=['Name', 'State', 'Electorate', 'Party', 'Screen_Name'])
    politician_list = temp_df['Screen_Name'].dropna().tolist()
    state_list = temp_df['State'].dropna().tolist()
    ele_list = temp_df['Electorate'].dropna().tolist()
    party_list = temp_df['Party'].dropna().tolist()
    for i in range(len(politician_list)):
        print('============================================')
        print('Process: {}/{}'.format(i + 1, len(politician_list)))
        restful_user_info = RestfulUserInfo(politician_list[i], state_list[i], ele_list[i], party_list[i], 'test',
                                            'politicianInfo')
        print("Crawling information of {}.".format(politician_list[i]))
        restful_user_info.start()
        restful_user_info.join()
