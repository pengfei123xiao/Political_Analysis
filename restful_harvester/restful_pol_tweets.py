#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 30/03/2019 7:06 PM
# @Author  : Pengfei Xiao
# @FileName: restful_pol_tweets.py
# @Software: PyCharm

"""This file is used to crawl politicians' tweets."""

import sys
import pandas as pd
from tweepy import API, TweepError
sys.path.append('..')
from config import config
from analyser import functional_tools
import threading
import gc

gc.enable()
conf = config[4]

class RestfulPolTweets(threading.Thread):
    def __init__(self, screen_name, db_name, collection_name, state_name, electorate_name,
                 party_name, start_date):
        threading.Thread.__init__(self)
        self.auth = functional_tools.FunctionalTools().authenticate_twitter_app(conf['consumer_key'], conf['consumer_secret'])
        self.twitter_client_api = API(self.auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True, timeout=200)
        self.SCREEN_NAME = screen_name
        self.db_name = db_name
        self.collection_name = collection_name
        self.electorate_name = electorate_name
        self.party_name = party_name
        self.state_name = state_name
        self.start_date = start_date

    def run(self):
        max_id = None
        TWEETS_PER_QUERY = 60
        records_count = 0
        f_tools = functional_tools.FunctionalTools()
        while True:
            try:
                raw_tweets = self.twitter_client_api.user_timeline(screen_name=self.SCREEN_NAME, tweet_mode='extended',
                                                                   count=TWEETS_PER_QUERY, max_id=max_id)
                if len(raw_tweets) == 0:
                    print("No more tweets found.")
                    print('In total {} tweets are stored in DB.'.format(records_count))
                    print('-----')
                    break
                max_id = raw_tweets[-1].id - 1  # update max_id to harvester earlier data
                df = f_tools.pol_tweets_to_dataframe(raw_tweets, self.state_name, self.electorate_name,
                                                     self.party_name)

                if df.shape[0] != 0:
                    records_count += df.shape[0]
                    f_tools.save_data(df.to_dict('records'), self.db_name, self.collection_name, 'update')
                if raw_tweets[-1].created_at < self.start_date:
                    print('Date boundary reached.')
                    print('In total {} tweets are stored in DB.'.format(records_count))
                    print('-----')
                    break

            except TweepError as e1:
                print('Restful tweets error:')
                print(e1)
                break

            except Exception as e2:
                print(e2)
                break


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
        restful_crawler = RestfulPolTweets(politician_list[i], 'test', 'test99', state_list[i], ele_list[i], party_list[i])
        print("Crawling tweets of {}.".format(politician_list[i]))
        restful_crawler.start()
        # restful_crawler.join()
