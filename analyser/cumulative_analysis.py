#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 8/05/2019 8:58 PM
# @Author  : Pengfei Xiao
# @FileName: cumulative_analysis.py
# @Software: PyCharm

import pandas as pd
import numpy as np
from datetime import datetime
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
    date_list = pd.date_range(start='2019-04-13', end='2019-4-15', freq='D')  # , tz='Australia/Sydney')
    result_dict = {}
    pol_result = []
    party_result = []
    for i in range(len(date_list) - 1):
        start = date_list[i]
        end = date_list[i + 1]

        # slicing dataframe via date
        totalMention_from_mongo = f_tools.find_mongo_by_date('capstone', 'streamingMentionedCorrectDate',
                                                             datetime(2019, 4, 13), end)
        totalMention_df = pd.DataFrame(list(totalMention_from_mongo))
        print('Mentioned data loaded. Time used: %f mins' % ((time.time() - start_time) / 60))

        # read cumulative politician tweets
        pol_tweets_from_mongo = f_tools.find_mongo_by_date('capstone', 'restfulTweets', datetime(2019, 4, 13), end)
        pol_tweets_df = pd.DataFrame(list(pol_tweets_from_mongo))

        cumulative_analysis = tweets_analysis.TweetsAnalysis(politician_df, totalMention_df, pol_tweets_df)
        extend_politician_df = cumulative_analysis.features_concat()
        print('{} features concat finished. Time used: {} mins'.format(datetime.strftime(start, '%b-%d-%Y'),
                                                                       (time.time() - start_time) / 60))
        del pol_tweets_from_mongo, totalMention_from_mongo, totalMention_df, pol_tweets_df
        gc.collect()
        # result_dict[datetime.strftime(start, '%b-%d-%Y')] = extend_politician_df.to_dict('records')
        # result.append({'date': datetime.strftime(start, '%b-%d-%Y'), 'data': extend_politician_df.to_dict('records')})
        pol_result.append(
            {'date': datetime.strftime(start, '%b-%d-%Y'), 'data': extend_politician_df.to_dict('records')})

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
        party_result.append(
            {'date': datetime.strftime(start, '%b-%d-%Y'), 'data': sum_party_df.to_dict('records')})

        del extend_politician_df, sum_party_df
        gc.collect()

    result_dict['sumPolitician'] = pol_result
    result_dict['sumParty'] = party_result
    # f_tools.save_data(result, 'test', 'test', 'insert_many')
    f_tools.save_data(result_dict, 'test', 'test1', 'insert_one')
