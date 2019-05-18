#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 6/05/2019 12:04 AM
# @Author  : Pengfei Xiao
# @FileName: daily_analysis.py
# @Software: PyCharm

import pandas as pd
import numpy as np
import datetime
import time
import sys

sys.path.append('..')
from analyser import functional_tools
from analyser import tweets_analysis
import gc

gc.enable()

if __name__ == '__main__':
    f_tools = functional_tools.FunctionalTools()
    start_time = time.time()
    print('Start read politician data')
    start_time = time.time()

    # read politician data from mongoDB
    politician_from_mongo = f_tools.find_data('capstone', 'politicianInfo')
    politician_df = pd.DataFrame(list(politician_from_mongo))

    # create daily timestamp
    start_date_str = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    end_date_str = datetime.date.today().strftime('%Y-%m-%d')
    # date_list = pd.date_range(start=start_date_str, end=end_date_str, freq='D')
    date_list = pd.date_range(start='2019-04-13', end='2019-04-21', freq='D')

    for i in range(len(date_list) - 1):
        start = date_list[i]
        end = date_list[i + 1]
        result_dict = {}

        # read daily data from mongoDB
        pol_tweets_mongo = f_tools.find_mongo_by_date('capstone', 'restfulTweets', start, end)
        pol_tweets_df = pd.DataFrame(list(pol_tweets_mongo))

        user_tweets_mongo = f_tools.find_mongo_by_date('capstone', 'restfulByHashtag', start, end)
        user_tweets_df = pd.DataFrame(list(user_tweets_mongo))

        totalMention_from_mongo = f_tools.find_mongo_by_date('capstone', 'streamingMentionedCorrectDate', start, end)
        totalMention_df = pd.DataFrame(list(totalMention_from_mongo))
        user_tweets_df = user_tweets_df.append(totalMention_df, ignore_index=True)
        print('Daily tweets loaded.')

        daily_analysis = tweets_analysis.TweetsAnalysis(politician_df, totalMention_df, pol_tweets_df)

        # ===daily statistical data analysis===
        extend_politician_df = daily_analysis.features_concat()

        # ===popular hashtags analysis===
        pol_top_tags = daily_analysis.count_popular_hashtag(pol_tweets_df)
        user_top_tags = daily_analysis.count_popular_hashtag(user_tweets_df)
        result_dict['date'] = datetime.datetime.strftime(start, '%b-%d-%Y')
        result_dict['data'] = {'Top_Tags_of_Politicians': pol_top_tags,
                               'Top_Tags_of_Users': user_top_tags,
                               'dailyPolitician': extend_politician_df.to_dict('records')}

        # f_tools.save_data(result_dict, 'test', 'dailyHead', 'insert_one')
        f_tools.save_data(result_dict, 'test', 'dailyTest', 'insert_one')

        del pol_tweets_mongo, pol_tweets_df, user_tweets_mongo, user_tweets_df, totalMention_from_mongo, totalMention_df
        del extend_politician_df
        gc.collect()
        print('{} daily analysis finished. Time used: {} mins'.format(datetime.datetime.strftime(start, '%b-%d-%Y'),
                                                                      (time.time() - start_time) / 60))
