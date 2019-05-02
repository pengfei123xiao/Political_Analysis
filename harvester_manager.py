#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 27/03/2019 12:28 AM
# @Author  : Pengfei Xiao
# @FileName: harvester_manager.py
# @Software: PyCharm

import pandas as pd
import time
from restful_harvester.restful_replies import RestfulReplies
from restful_harvester.restful_pol_tweets import RestfulPolTweets
from restful_harvester.restful_by_hashtag import RestfulHashtags
from restful_harvester.restful_by_mentioned import RestfulByMentioned
import gc

gc.enable()

if __name__ == '__main__':
    start_time = time.time()

    temp_df = pd.read_csv('data/new_politician_list.csv', usecols=['ScreenName'])
    politician_list = temp_df['ScreenName'].dropna().tolist()

    print("Start restful crawling.")
    # for hashtag in hashtag_list:
    #     print('============================================')
    #     print('Process: {}/{}'.format(hashtag_list.index(hashtag) + 1, len(hashtag_list)))
    #     restful_hashtag = RestfulHashtags(hashtag)
    #     print("Crawling tweets by {}.".format(hashtag))
    #     restful_hashtag.start()
    #     restful_hashtag.join()
    for screen_name in politician_list:  # ['HelenHaines1', 'RobertHolian']:
        print('============================================')
        print('Process: {}/{}'.format(politician_list.index(screen_name) + 1, len(politician_list)))
        restful_mentioned = RestfulByMentioned(screen_name, 'capstone', 'restfulMentioned')
        restful_replies = RestfulReplies(screen_name, 'capstone', 'restfulMentioned')
        # restful_pol_tweets = RestfulPolTweets(screen_name, 'capstone', 'restfulTweets')
        print("Crawling tweets mentioned {}.".format(screen_name))
        restful_mentioned.start()
        # print("Crawling tweets from {}.".format(screen_name))
        # restful_pol_tweets.start()
        print("Crawling replies to {}.".format(screen_name))
        restful_replies.start()
        restful_replies.join()
        # restful_pol_tweets.join()
        restful_mentioned.join()

    print('Finished. Time used: %f mins' % ((time.time() - start_time) / 60))

    gc.collect()
