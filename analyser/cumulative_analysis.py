#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 8/05/2019 8:58 PM
# @Author  : Pengfei Xiao
# @FileName: cumulative_analysis.py
# @Software: PyCharm

import sys
import time
from datetime import datetime, timedelta

import pandas as pd

sys.path.append('..')
from analyser import tweets_analysis
import threading
import gc

gc.enable()


class CumulativeAnalysis(threading.Thread):
    """

    """

    def __init__(self, start_date, end_date, f_tools, politician_df):
        """
        """
        threading.Thread.__init__(self)
        self.start_date = start_date
        self.end_date = end_date
        self.f_tools = f_tools
        self.politician_df = politician_df

    def run(self):
        start_time = time.time()
        pol_tweets_from_mongo = self.f_tools.find_mongo_by_date("backup", 'restfulTweets', datetime(2019, 4, 13),
                                                                self.end_date)
        pol_tweets_df = pd.DataFrame(list(pol_tweets_from_mongo))
        del pol_tweets_from_mongo
        gc.collect()
        mentionState_from_mongo = self.f_tools.find_mongo_by_date('backup', 'totalMentionedWithState',
                                                                  datetime(2019, 4, 13), self.end_date)
        mentionState_df = pd.DataFrame(list(mentionState_from_mongo))  # mentions with state info
        del mentionState_from_mongo
        gc.collect()
        state_list = ['New South Wales', 'Victoria', 'Queensland', 'South Australia', 'Western Australia',
                      'Northern Territory', 'Australian Capital Territory', 'Other Territories', 'Tasmania']
        for i in range((self.end_date - self.start_date).days):
            result_dict = {}
            # start = self.start_date + timedelta(days=i)
            end = self.start_date + timedelta(days=i + 1)
            new_mention_df = mentionState_df[mentionState_df['Date'] < end]
            # new_mention_df = self.totalMention_df[self.totalMention_df['Date'] < end]
            print(new_mention_df.shape)
            new_pol_tweets_df = pol_tweets_df[pol_tweets_df['Date'] < end]

            cumulative_analysis = tweets_analysis.TweetsAnalysis(self.politician_df, new_mention_df, new_pol_tweets_df)
            print(new_mention_df.shape)
            extend_politician_df = cumulative_analysis.features_concat()
            for _, v in extend_politician_df.iterrows():
                for state in state_list:
                    if state not in v['State_Pos'][0]:
                        v['State_Pos'][0][state] = 0
                    if state not in v['State_Neu'][0]:
                        v['State_Neu'][0][state] = 0
                    if state not in v['State_Neg'][0]:
                        v['State_Neg'][0][state] = 0

            print('{} features concat finished. Time used: {} mins'.format(datetime.strftime(end, '%b-%d-%Y'),
                                                                           (time.time() - start_time) / 60))
            del new_pol_tweets_df, new_mention_df
            gc.collect()

            # ===cumulative party data===
            # temp_df = extend_politician_df.copy()
            # temp_df.drop(columns=['Avatar', 'Create_Time', 'Description', 'Electoral_District', 'ID',
            #                       'Location', 'Name', 'Screen_Name', 'State', '_id'], inplace=True)
            # extend_politician_df.drop(columns=['Avatar', 'Create_Time', 'Description', 'Electoral_District', 'ID',
            #                                    'Location', 'Name', 'Screen_Name', 'State', '_id'], inplace=True)
            """drop后报screen_name key error，待解决"""
            sum_party_df = extend_politician_df.groupby(by='Party').agg('sum')
            sum_party_df['Word_Cloud'] = sum_party_df.index.map(
                lambda x: cumulative_analysis.word_frequency(x, 'Party'))
            sum_party_df['Party'] = sum_party_df.index
            # state sentiment distribution
            sum_party_df['State_Pos'] = sum_party_df['Party'].apply(
                lambda x: cumulative_analysis.state_sentiment_party(extend_politician_df, x, 'State_Pos'))
            sum_party_df['State_Neu'] = sum_party_df['Party'].apply(
                lambda x: cumulative_analysis.state_sentiment_party(extend_politician_df, x, 'State_Neu'))
            sum_party_df['State_Neg'] = sum_party_df['Party'].apply(
                lambda x: cumulative_analysis.state_sentiment_party(extend_politician_df, x, 'State_Neg'))

            result_dict['date'] = datetime.strftime(end + timedelta(days=1), '%b-%d-%Y')
            result_dict['data'] = {'sumPolitician': extend_politician_df.to_dict('records'),
                                   'sumParty': sum_party_df.to_dict('records')
                                   }
            # f_tools.save_data(result_dict, 'test', 'sumHead', 'insert_one')
            self.f_tools.save_data(result_dict, 'test', 'sumTest', 'insert_one', '103.6.254.48/')

            del extend_politician_df, sum_party_df
        gc.collect()
