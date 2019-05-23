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
        pol_tweets = self.f_tools.find_mongo_by_date("backup", 'restfulTweets', datetime(2019, 4, 13), self.end_date)
        pol_tweets_df = pd.DataFrame(list(pol_tweets))
        del pol_tweets
        gc.collect()
        mentionState_from_mongo = self.f_tools.find_mongo_by_date('backup', 'totalMentionedWithState',
                                                                  datetime(2019, 4, 13), self.end_date
                                                                  )
        mentionState_df = pd.DataFrame(list(mentionState_from_mongo))  # mentions with state info
        del mentionState_from_mongo
        gc.collect()
        for i in range((self.end_date - self.start_date).days):
            result_dict = {}
            # start = self.start_date + timedelta(days=i)
            end = self.start_date + timedelta(days=i + 1)
            new_mention_df = mentionState_df[mentionState_df['Date'] < end]
            # new_mention_df = self.totalMention_df[self.totalMention_df['Date'] < end]

            new_pol_tweets_df = pol_tweets_df[pol_tweets_df['Date'] < end]

            cumulative_analysis = tweets_analysis.TweetsAnalysis(self.politician_df, new_mention_df, new_pol_tweets_df)
            extend_politician_df = cumulative_analysis.features_concat()
            print('{} features concat finished. Time used: {} mins'.format(datetime.strftime(start, '%b-%d-%Y'),
                                                                           (time.time() - start_time) / 60))

            # ===sentiment from state===
            new_mentionState_df = new_mention_df[new_mention_df['Date'] < end]
            new_mentionState_df.dropna(subset=['State'], inplace=True)
            pos_mentionState_df = new_mention_df[new_mention_df['Content_Sentiment'] == 1]
            neu_mentionState_df = new_mention_df[new_mention_df['Content_Sentiment'] == 0]
            neg_mentionState_df = new_mention_df[new_mention_df['Content_Sentiment'] == -1]

            del new_mention_df, new_pol_tweets_df

            # new_mentionState_df = self.mentionState_df[self.mentionState_df['Date'] < end]
            # new_mentionState_df.dropna(subset=['State'], inplace=True)
            # pos_mentionState_df = self.mentionState_df[self.mentionState_df['Content_Sentiment'] == 1]
            # neu_mentionState_df = self.mentionState_df[self.mentionState_df['Content_Sentiment'] == 0]
            # neg_mentionState_df = self.mentionState_df[self.mentionState_df['Content_Sentiment'] == -1]

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
            result_dict['date'] = datetime.strftime(end + timedelta(days=1), '%b-%d-%Y')
            result_dict['data'] = {'sumPolitician': extend_politician_df.to_dict('records'),
                                   'sumParty': sum_party_df.to_dict('records'),
                                   'State_Pos': (pos_mentionState_df.groupby(by='State')['ID'].count()).to_dict(),
                                   'State_Neu': (neu_mentionState_df.groupby(by='State')['ID'].count()).to_dict(),
                                   'State_Neg': (neg_mentionState_df.groupby(by='State')['ID'].count()).to_dict()}
            # f_tools.save_data(result_dict, 'test', 'sumHead', 'insert_one')
            self.f_tools.save_data(result_dict, 'test', 'sumTest', 'insert_one', '103.6.254.48/')

            del extend_politician_df, sum_party_df, new_mentionState_df, pos_mentionState_df, neu_mentionState_df, neg_mentionState_df
        gc.collect()

# if __name__ == '__main__':
# f_tools = functional_tools.FunctionalTools()
# print('Start read politician data')
# start_time = time.time()
# # read data from mongoDB
# politician_from_mongo = f_tools.find_data('capstone', 'politicianInfo')
# politician_df = pd.DataFrame(list(politician_from_mongo))
#
# del politician_from_mongo
# gc.collect()
#
# # create daily timestamp
# start_date_str = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')
# end_date_str = date.today().strftime('%Y-%m-%d')
# # date_list = pd.date_range(start=start_date_str, end=end_date_str, freq='D')
# date_list = pd.date_range(start='2019-04-13', end='2019-04-21', freq='D')
#
# for i in range(len(date_list) - 1):
#     result_dict = {}
#     start = date_list[i]
#     end = date_list[i + 1]
#
#     # slicing dataframe via date
#     totalMention_from_mongo = f_tools.find_mongo_by_date('capstone', 'streamingMentionedCorrectDate',
#                                                          datetime(2019, 4, 13), end)
#     totalMention_df = pd.DataFrame(list(totalMention_from_mongo))
#     print('Mentioned data loaded. Time used: %f mins' % ((time.time() - start_time) / 60))
#
#     # read cumulative politician tweets
#     pol_tweets_from_mongo = f_tools.find_mongo_by_date('capstone', 'restfulTweets', datetime(2019, 4, 13),
#                                                        end)
#     pol_tweets_df = pd.DataFrame(list(pol_tweets_from_mongo))
#
#     cumulative_analysis = tweets_analysis.TweetsAnalysis(politician_df, totalMention_df, pol_tweets_df)
#     extend_politician_df = cumulative_analysis.features_concat()
#     print('{} features concat finished. Time used: {} mins'.format(datetime.strftime(start, '%b-%d-%Y'),
#                                                                    (time.time() - start_time) / 60))
#     del pol_tweets_from_mongo, totalMention_from_mongo, totalMention_df, pol_tweets_df
#     gc.collect()
#
#     # ===sentiment from state===
#     mentionState_from_mongo = f_tools.find_mongo_by_date('backup', 'totalMentionedWithState',
#                                                          datetime(2019, 4, 13), end)
#     mentionState_df = pd.DataFrame(list(mentionState_from_mongo))  # mentions with state info
#     mentionState_df.dropna(subset=['State'], inplace=True)
#     pos_mentionState_df = mentionState_df[mentionState_df['Content_Sentiment'] == 1]
#     neu_mentionState_df = mentionState_df[mentionState_df['Content_Sentiment'] == 0]
#     neg_mentionState_df = mentionState_df[mentionState_df['Content_Sentiment'] == -1]
#
#     # ===cumulative party data===
#     # temp_df = extend_politician_df.copy()
#     # temp_df.drop(columns=['Avatar', 'Create_Time', 'Description', 'Electoral_District', 'ID',
#     #                       'Location', 'Name', 'Screen_Name', 'State', '_id'], inplace=True)
#     # extend_politician_df.drop(columns=['Avatar', 'Create_Time', 'Description', 'Electoral_District', 'ID',
#     #                                    'Location', 'Name', 'Screen_Name', 'State', '_id'], inplace=True)
#     """drop后报screen_name key error，待解决"""
#     sum_party_df = extend_politician_df.groupby(by='Party').agg('sum')
#     sum_party_df['Word_Cloud'] = sum_party_df.index.map(lambda x: cumulative_analysis.word_frequency(x, 'Party'))
#     sum_party_df['Party'] = sum_party_df.index
#     result_dict['date'] = datetime.strftime(start, '%b-%d-%Y')
#     result_dict['data'] = {'sumPolitician': extend_politician_df.to_dict('records'),
#                            'sumParty': sum_party_df.to_dict('records'),
#                            'State_Pos': (pos_mentionState_df.groupby(by='State')['ID'].count()).to_dict(),
#                            'State_Neu': (neu_mentionState_df.groupby(by='State')['ID'].count()).to_dict(),
#                            'State_Neg': (neg_mentionState_df.groupby(by='State')['ID'].count()).to_dict()}
#     # f_tools.save_data(result_dict, 'test', 'sumHead', 'insert_one')
#     f_tools.save_data(result_dict, 'test', 'sumTest', 'insert_one')
#
#     del extend_politician_df, sum_party_df, mentionState_df, mentionState_from_mongo, pos_mentionState_df
#     del neu_mentionState_df, neg_mentionState_df
#     gc.collect()
