#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 12/04/2019 10:51 PM
# @Author  : Pengfei Xiao
# @FileName: restful_by_hashtag.py
# @Software: PyCharm

from tweepy import OAuthHandler, AppAuthHandler, TweepError, API
import pandas as pd
import sys
sys.path.append('..')
from analyser import tweet_analyser
from ast import literal_eval  # Convert list-like string to list
from multiprocessing import Process
import threading
import time
import gc

gc.enable()

# Twitter API Keys-Siyu
# CONSUMER_KEY = "wWFHsJ71LrXoX0LRFNCVYxLoY"
# CONSUMER_SECRET = "dpOn4LvtZ0MqxgtFZB0XXFKz9wK7csAHLkusJ8JasUJIxFt6Qm"
# ACCESS_TOKEN = "1104525213847318529-S0OLx8OztXjSxeGCGITcGhVa2EMz5b"
# ACCESS_TOKEN_SECRET = "wEAjXPqWPygScOzAc8RRwiHzeg1G0mGVt20qZLoJGQuDe"


# Twitter API Keys-yiru
CONSUMER_KEY = '9uWwELoYRA4loNboCqe4P7XZD'
CONSUMER_SECRET = 'ZhIOn2XPAnVtDjbh4iVrANG4gq7zTCJdJZAAlDpPmKAFpNz4gF'
ACCESS_TOKEN = '2344719422-4a94VSU2kjHzgFp1Kap9uoAAvE5R2n9vb4H5Atz'
ACCESS_TOKEN_SECRET = 'O5H5r7QyOTct7yFFlePITJGcuIJPBmgyDBunIYRVjYELq'


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
        auth = AppAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)  # higher rate limit than OAuthHandler
        return auth


# class RestfulCrawler(Process):
class RestfulHashtags(threading.Thread):
    """

    """

    def __init__(self, hashtag, db_name, collection_name):
        """
        :param twitter_user:
        """
        # super().__init__()
        threading.Thread.__init__(self)
        self.auth = TwitterAuthenticator().authenticate_twitter_app()
        self.twitter_api = API(self.auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True, timeout=200)
        self.hashtag = hashtag
        self.db_name = db_name
        self.collection_name = collection_name

    def run(self):

        max_id = None
        NUM_PER_QUERY = 100
        records_count = 0
        analyser = tweet_analyser.TweetAnalyser()

        while True:
            try:
                raw_tweets = self.twitter_api.search(q='#' + self.hashtag,  # result_type='mixed',
                                                     tweet_mode='extended', max_id=max_id, count=NUM_PER_QUERY)
                if len(raw_tweets) == 0:
                    print("No more hashtag tweets found.")
                    print('In total {} tweets are stored in DB.'.format(records_count))
                    print('-----')
                    break

                max_id = raw_tweets[-1].id - 1  # update max_id to harvester earlier data
                df = analyser.tweets_to_dataframe(raw_tweets)

                if df.shape[0] != 0:
                    analyser.save_data(df.to_dict('records'), self.db_name, self.collection_name, 'update')
                    records_count += df.shape[0]

            except TweepError as e:
                print('Restful hashtag error:')
                print(e)
                break

            except Exception as e2:
                print(e2)
                break


if __name__ == '__main__':
    start_time = time.time()
    top_tag_df = pd.read_csv('../data/daily_top_tags.csv')
    hashtag_list = []
    for i, v in top_tag_df['Top_Tags_of_Politicians'].iteritems():
        for item in literal_eval(v):
            hashtag_list.append(item[0])
    hashtag_set = set(hashtag_list)

    count = 1
    for hashtag in hashtag_set:  # ['puthatelast']:
        print('============================================')
        print('Process: {}/{}'.format(count, len(hashtag_set)))
        restful_hashtag = RestfulHashtags(hashtag, 'capstone', 'restfulByHashtag')
        # restful_hashtag = RestfulHashtags(hashtag, 'test', 'test1')
        print("Crawling tweets by {}.".format(hashtag))
        restful_hashtag.start()
        restful_hashtag.join()
        count += 1
    print('Finished. Time used: %f mins' % ((time.time() - start_time) / 60))
