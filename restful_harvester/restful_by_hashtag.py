#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 12/04/2019 10:51 PM
# @Author  : Pengfei Xiao
# @FileName: restful_by_hashtag.py
# @Software: PyCharm

import sys

from tweepy import AppAuthHandler, TweepError, API

sys.path.append('..')
from analyser import functional_tools
import datetime
import threading
import gc

gc.enable()

# Twitter API Keys-Siyu
CONSUMER_KEY = "wWFHsJ71LrXoX0LRFNCVYxLoY"
CONSUMER_SECRET = "dpOn4LvtZ0MqxgtFZB0XXFKz9wK7csAHLkusJ8JasUJIxFt6Qm"
ACCESS_TOKEN = "1104525213847318529-S0OLx8OztXjSxeGCGITcGhVa2EMz5b"
ACCESS_TOKEN_SECRET = "wEAjXPqWPygScOzAc8RRwiHzeg1G0mGVt20qZLoJGQuDe"


# Twitter API Keys-yiru1
# ACCESS_TOKEN = "987908747375726593-yp9g2tc5FJMfr54eIc8mI8mkboX9VNC"
# ACCESS_TOKEN_SECRET = "hH5K46mLTHk5Yn51gesTnrq3YYUN5vGOBDjWx8PdBbdEm"
# CONSUMER_KEY = "1RcIwPa92JvKPegrsRzSWzXht"
# CONSUMER_SECRET = "AFceGbD4TzRGmqRz8oq65ovHIsAuG7WlwkzfsqEvz5WgYJDoUw"


# Twitter API Keys-yiru
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
        auth = AppAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)  # higher rate limit than OAuthHandler
        return auth


# class RestfulCrawler(Process):
class RestfulHashtags(threading.Thread):
    """

    """

    def __init__(self, start_date, hashtags, db_name, collection_name):
        """
        :param twitter_user:
        """
        # super().__init__()
        threading.Thread.__init__(self)
        self.auth = TwitterAuthenticator().authenticate_twitter_app()
        self.twitter_api = API(self.auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True, timeout=200)
        self.hashtags = hashtags
        self.db_name = db_name
        self.collection_name = collection_name
        self.start_date = start_date

    def run(self):
        max_id = None
        NUM_PER_QUERY = 100
        records_count = 0
        # start_date = datetime.datetime.today() - datetime.timedelta(days=1)
        f_tools = functional_tools.FunctionalTools()

        while True:
            try:
                query = ' OR '.join(self.hashtags)
                raw_tweets = self.twitter_api.search(q=query,  # result_type='mixed',
                                                     tweet_mode='extended', max_id=max_id, count=NUM_PER_QUERY)
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
                # break  # test only
            except TweepError as e:
                print('Restful hashtag error:')
                print(e)
                break

            except Exception as e2:
                print(e2)
                break

# if __name__ == '__main__':
#     start_time = time.time()
#     # ===update politician tweets===
#     # temp_df = pd.read_csv('../data/full_politician_list.csv',
#     #                       usecols=['Name', 'State', 'Electorate', 'Party', 'Screen_Name'])
#     # politician_list = temp_df['Screen_Name'].dropna().tolist()
#     # state_list = temp_df['State'].dropna().tolist()
#     # ele_list = temp_df['Electorate'].dropna().tolist()
#     # party_list = temp_df['Party'].dropna().tolist()
#     # for i in range(len(politician_list)):
#     #     print('============================================')
#     #     print('Process: {}/{}'.format(i + 1, len(politician_list)))
#     #     restful_crawler = RestfulPolTweets(politician_list[i], 'capstone', 'restfulTweets', state_list[i], ele_list[i],
#     #                                        party_list[i])
#     #     print("Crawling tweets of {}.".format(politician_list[i]))
#     #     restful_crawler.start()
#     #     restful_crawler.join()
#
#     #  ===update politician tweets===
#     top_tag_df = pd.read_csv('../data/daily_top_tags.csv')
#     hashtag_list = []
#     for i, v in top_tag_df['Top_Tags_of_Politicians'].iteritems():
#         for item in literal_eval(v):
#             hashtag_list.append(item[0])
#     hashtag_set = set(hashtag_list)
#     count = 1
#     hash_list1 = ['peoplesforum',
#                   'widebayvotes',
#                   'buildingoureconomy',
#                   'climatechange',
#                   'laborwide',
#                   'leadersdebate',
#                   'vote1yates',
#                   'wentworthvotes',
#                   'cowper',
#                   'warringahdebate',
#                   'abbottvsteggall',
#                   'mymum',
#                   'laborlaunch',
#                   'greens',
#                   'mothersday',
#                   'ausvotes',
#                   'kooyongvotes',
#                   'qanda']
#     for hashtag in hash_list1:  # hashtag_set:  # hashtag_set:  # ['puthatelast']:
#         print('============================================')
#         print('Process: {}/{}'.format(count, len(hash_list1)))
#         restful_hashtag = RestfulHashtags(hashtag, 'capstone', 'restfulByHashtag')
#         # restful_hashtag = RestfulHashtags(hashtag, 'test', 'test99')
#         print("Crawling tweets by {}.".format(hashtag))
#         restful_hashtag.start()
#         restful_hashtag.join()
#         count += 1
#     print('Finished. Time used: %f mins' % ((time.time() - start_time) / 60))
