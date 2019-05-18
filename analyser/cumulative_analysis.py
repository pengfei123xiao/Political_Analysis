#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 8/05/2019 8:58 PM
# @Author  : Pengfei Xiao
# @FileName: cumulative_analysis.py
# @Software: PyCharm

import pandas as pd
import numpy as np
import datetime
import time
import swifter
import sys

sys.path.append('..')
from analyser import functional_tools
from analyser import tweets_analysis
import gc

gc.enable()

if __name__ == '__main__':
    f_tools = functional_tools.FunctionalTools()
    print('Start read politician data')
    start_time = time.time()
    # read data from mongoDB
    politician_from_mongo = f_tools.find_data('capstone', 'politicianInfo')
    politician_df = pd.DataFrame(list(politician_from_mongo))

    del politician_from_mongo
    gc.collect()

    # create daily timestamp
    start_date_str = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    end_date_str = datetime.date.today().strftime('%Y-%m-%d')
    date_list = pd.date_range(start=start_date_str, end=end_date_str, freq='D')

    # date_list = pd.date_range(start='2019-05-17', end='2019-05-18', freq='D')

    for i in range(len(date_list) - 1):
        result_dict = {}
        start = date_list[i]
        end = date_list[i + 1]

        # slicing dataframe via date
        totalMention_from_mongo = f_tools.find_mongo_by_date('capstone', 'streamingMentionedCorrectDate',
                                                             datetime.datetime(2019, 4, 13), end)
        totalMention_df = pd.DataFrame(list(totalMention_from_mongo))
        print('Mentioned data loaded. Time used: %f mins' % ((time.time() - start_time) / 60))

        # read cumulative politician tweets
        pol_tweets_from_mongo = f_tools.find_mongo_by_date('capstone', 'restfulTweets', datetime.datetime(2019, 4, 13),
                                                           end)
        pol_tweets_df = pd.DataFrame(list(pol_tweets_from_mongo))

        cumulative_analysis = tweets_analysis.TweetsAnalysis(politician_df, totalMention_df, pol_tweets_df)
        extend_politician_df = cumulative_analysis.features_concat()
        print('{} features concat finished. Time used: {} mins'.format(datetime.datetime.strftime(start, '%b-%d-%Y'),
                                                                       (time.time() - start_time) / 60))
        del pol_tweets_from_mongo, totalMention_from_mongo, totalMention_df, pol_tweets_df
        gc.collect()

        # ===cumulative party data===
        # temp_df = extend_politician_df.copy()
        # temp_df.drop(columns=['Avatar', 'Create_Time', 'Description', 'Electoral_District', 'ID',
        #                       'Location', 'Name', 'Screen_Name', 'State', '_id'], inplace=True)
        # extend_politician_df.drop(columns=['Avatar', 'Create_Time', 'Description', 'Electoral_District', 'ID',
        #                                    'Location', 'Name', 'Screen_Name', 'State', '_id'], inplace=True)
        """drop后报screen_name key error，待解决"""
        sum_party_df = extend_politician_df.groupby(by='Party').agg('sum')
        sum_party_df['Word_Cloud'] = sum_party_df.index.map(lambda x: cumulative_analysis.word_frequency(x, 'Party'))
        sum_party_df['Party'] = sum_party_df.index
        result_dict['date'] = datetime.strftime(start, '%b-%d-%Y')
        result_dict['data'] = {'sumPolitician': extend_politician_df.to_dict('records'),
                               'sumParty': sum_party_df.to_dict('records')}
        f_tools.save_data(result_dict, 'test', 'sumHead', 'insert_one')

        del extend_politician_df, sum_party_df
        gc.collect()
