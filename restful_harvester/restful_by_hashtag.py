#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 12/04/2019 10:51 PM
# @Author  : Pengfei Xiao
# @FileName: restful_by_hashtag.py
# @Software: PyCharm

"""This file is used to crawl hashtag-related tweets."""

import sys
sys.path.append('..')
from config import config
from analyser import functional_tools
from tweepy import TweepError, API
import threading
import gc

gc.enable()
conf = config[1]

class RestfulHashtags(threading.Thread):
    def __init__(self, start_date, hashtags, db_name, collection_name):
        threading.Thread.__init__(self)
        self.auth = functional_tools.FunctionalTools().authenticate_twitter_app(conf['consumer_key'], conf['consumer_secret'])
        self.twitter_api = API(self.auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True, timeout=200)
        self.hashtags = hashtags
        self.db_name = db_name
        self.collection_name = collection_name
        self.start_date = start_date

    def run(self):
        max_id = None
        NUM_PER_QUERY = 100
        records_count = 0
        f_tools = functional_tools.FunctionalTools()

        while True:
            try:
                query = ' OR '.join(self.hashtags)
                raw_tweets = self.twitter_api.search(q=query, result_type='mixed', tweet_mode='extended', max_id=max_id,
                                                     count=NUM_PER_QUERY)
                if len(raw_tweets) == 0:
                    print("No more hashtag tweets found.")
                    print('In total {} tweets are stored in DB.'.format(records_count))
                    print('-----')
                    break

                max_id = raw_tweets[-1].id - 1  # update max_id to harvester earlier data
                df = f_tools.tweets_to_dataframe(raw_tweets)

                if df.shape[0] != 0:
                    f_tools.save_data(df.to_dict('records'), self.db_name, self.collection_name, 'update')
                    records_count += df.shape[0]
                if raw_tweets[-1].created_at < self.start_date:
                    print('Date boundary reached.')
                    print('In total {} tweets are stored in DB.'.format(records_count))
                    print('-----')
                    break
            except TweepError as e:
                print('Restful hashtag error:')
                print(e)
                break

            except Exception as e2:
                print(e2)
                break
