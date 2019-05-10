#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 6/05/2019 12:04 AM
# @Author  : Pengfei Xiao
# @FileName: daily_analysis.py
# @Software: PyCharm

import pandas as pd
import numpy as np
from datetime import datetime
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
    date_list = pd.date_range(start='2019-04-13', end='2019-04-15', freq='D')
    result_dict = {}
    pol_result = []
    pol_toptag_result = []
    user_toptag_result = []
    for i in range(len(date_list) - 1):
        start = date_list[i]
        end = date_list[i + 1]

        # read daily data from mongoDB
        pol_tweets_mongo = f_tools.find_mongo_by_date('capstone', 'restfulTweets', start, end)
        pol_tweets_df = pd.DataFrame(list(pol_tweets_mongo))

        user_tweets_mongo = f_tools.find_mongo_by_date('capstone', 'restfulByHashtag', start, end)
        user_tweets_df = pd.DataFrame(list(user_tweets_mongo))

        totalMention_from_mongo = f_tools.find_mongo_by_date('capstone', 'streamingMentionedCorrectDate', start, end)
        totalMention_df = pd.DataFrame(list(totalMention_from_mongo))
        print('Daily tweets loaded.')

        daily_analysis = tweets_analysis.TweetsAnalysis(politician_df, totalMention_df, pol_tweets_df)
        # ===daily statistical data analysis===
        extend_politician_df = daily_analysis.features_concat()
        pol_result.append(
            {'date': datetime.strftime(start, '%b-%d-%Y'), 'data': extend_politician_df.to_dict('records')})

        # ===popular hashtags analysis===
        pol_top_tags = daily_analysis.count_popular_hashtag(pol_tweets_df)
        user_top_tags = daily_analysis.count_popular_hashtag(user_tweets_df)
        pol_toptag_result.append({'date': datetime.strftime(start, '%b-%d-%Y'), 'data': pol_top_tags})
        user_toptag_result.append({'date': datetime.strftime(start, '%b-%d-%Y'), 'data': user_top_tags})

        del pol_tweets_mongo, pol_tweets_df, user_tweets_mongo, user_tweets_df, totalMention_from_mongo, totalMention_df
        del extend_politician_df
        gc.collect()
        print('{} daily analysis finished. Time used: {} mins'.format(datetime.strftime(start, '%b-%d-%Y'),
                                                                      (time.time() - start_time) / 60))

    result_dict['Top_Tags_of_Politicians'] = pol_toptag_result
    result_dict['Top_Tags_of_Users'] = user_toptag_result
    result_dict['Daily_Politician'] = pol_result
    f_tools.save_data(result_dict, 'test', 'test1', 'insert_one')
