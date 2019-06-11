#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 30/04/2019 3:59 PM
# @Author  : Pengfei Xiao
# @FileName: restful_pol_info.py
# @Software: PyCharm

"""This file is used to crawl politicians' account information."""

from tweepy import API, TweepError
import pandas as pd
import sys

sys.path.append('..')
from analyser import functional_tools
import threading
import datetime
import gc

gc.enable()

# Twitter API Keys-yiru
CONSUMER_KEY = '9uWwELoYRA4loNboCqe4P7XZD'
CONSUMER_SECRET = 'ZhIOn2XPAnVtDjbh4iVrANG4gq7zTCJdJZAAlDpPmKAFpNz4gF'
ACCESS_TOKEN = '2344719422-4a94VSU2kjHzgFp1Kap9uoAAvE5R2n9vb4H5Atz'
ACCESS_TOKEN_SECRET = 'O5H5r7QyOTct7yFFlePITJGcuIJPBmgyDBunIYRVjYELq'


class RestfulUserInfo(threading.Thread):
    def __init__(self, screen_name, db_name, collection_name, state_name, electorate_name, party_name):
        threading.Thread.__init__(self)
        self.auth = functional_tools.FunctionalTools().authenticate_twitter_app(CONSUMER_KEY, CONSUMER_SECRET)
        self.twitter_api = API(self.auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True, timeout=200)
        self.SCREEN_NAME = screen_name
        self.db_name = db_name
        self.collection_name = collection_name
        self.state_name = state_name
        self.electorate_name = electorate_name
        self.party_name = party_name

    def run(self):
        f_tools = functional_tools.FunctionalTools()
        try:
            user_info = self.twitter_api.get_user(self.SCREEN_NAME)
            if not user_info:
                print("No user information found.")
                print('-----')

            df = f_tools.politician_info_to_dataframe(user_info, self.state_name, self.electorate_name,
                                                      self.party_name)

            if df.shape[0] != 0:
                f_tools.save_data(df.to_dict('records'), self.db_name, self.collection_name, 'update')

        except TweepError as e1:
            print('Restful by user info error:')
            print(e1)

        except Exception as e2:
            print(e2)


""" testing """
if __name__ == "__main__":
    f_tools = functional_tools.FunctionalTools()
    temp_df = pd.read_csv('../data/full_politician_list.csv',
                          usecols=['Name', 'State', 'Electorate', 'Party', 'Screen_Name'])
    politician_list = temp_df['Screen_Name'].dropna().tolist()
    state_list = temp_df['State'].dropna().tolist()
    ele_list = temp_df['Electorate'].dropna().tolist()
    party_list = temp_df['Party'].dropna().tolist()
    result_dict = {}
    for i in range(len(politician_list)):
        print('============================================')
        print('Process: {}/{}'.format(i + 1, len(politician_list)))
        restful_user_info = RestfulUserInfo(politician_list[i], 'test', 'test', state_list[i], ele_list[i],
                                            party_list[i])
        print("Crawling information of {}.".format(politician_list[i]))
        restful_user_info.start()
        restful_user_info.join()

    politician_from_mongo = f_tools.find_data('backup', 'politicianInfo')
    politician_df = pd.DataFrame(list(politician_from_mongo))
    result_dict['Date'] = datetime.date.today()
    result_dict['data'] = politician_df.to_dict('records')
    f_tools.save_data(result_dict, 'backup', 'dailyPolInfo', 'insert_one')
