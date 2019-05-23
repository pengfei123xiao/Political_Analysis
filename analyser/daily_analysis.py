#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 6/05/2019 12:04 AM
# @Author  : Pengfei Xiao
# @FileName: daily_analysis.py
# @Software: PyCharm

import datetime
import sys
import threading
import time
from datetime import datetime, timedelta

import pandas as pd

sys.path.append('..')
from analyser import tweets_analysis
import gc

gc.enable()


class DailyAnalysis(threading.Thread):
    """

    """

    def __init__(self, start_date, end_date, f_tools, totalMention_df, pol_tweets_df, politician_df):
        """
        """
        threading.Thread.__init__(self)
        self.start_date = start_date
        self.end_date = end_date
        self.f_tools = f_tools
        self.totalMention_df = totalMention_df
        self.pol_tweets_df = pol_tweets_df
        self.politician_df = politician_df

    def run(self):
        # daily analysis
        for i in range((self.end_date - self.start_date).days):
            start = self.start_date + timedelta(days=i)
            end = self.start_date + timedelta(days=i + 1)
            result_dict = {}
            print('{} daily analysis starts.'.format(datetime.strftime(start, '%b-%d-%Y')))
            start_time = time.time()
            # read daily data from mongo
            new_pol_tweets_df = self.pol_tweets_df[
                (self.pol_tweets_df['Date'] >= start) & (self.pol_tweets_df['Date'] < end)]

            user_hashtag_tweets_mongo = self.f_tools.find_mongo_by_date('backup', 'restfulByHashtag', start, end)
            user_hashtag_tweets_df = pd.DataFrame(list(user_hashtag_tweets_mongo))
            del user_hashtag_tweets_mongo

            new_mention_df = self.totalMention_df[
                (self.totalMention_df['Date'] >= start) & (self.totalMention_df['Date'] < end)]
            user_hashtag_tweets_df = user_hashtag_tweets_df.append(new_mention_df, ignore_index=True)
            print('Daily tweets loaded.')

            daily_analysis = tweets_analysis.TweetsAnalysis(self.politician_df, new_mention_df, new_pol_tweets_df)

            # ===daily statistical data analysis===
            extend_politician_df = daily_analysis.features_concat()

            # ===popular hashtags analysis===
            pol_top_tags = self.f_tools.count_popular_hashtag(new_pol_tweets_df)
            user_top_tags = self.f_tools.count_popular_hashtag(user_hashtag_tweets_df)
            result_dict['date'] = datetime.strftime(end, '%b-%d-%Y')
            result_dict['data'] = {'Top_Tags_of_Politicians': pol_top_tags,
                                   'Top_Tags_of_Users': user_top_tags,
                                   'dailyPolitician': extend_politician_df.to_dict('records')}

            # f_tools.save_data(result_dict, 'test', 'dailyHead', 'insert_one')
            self.f_tools.save_data(result_dict, 'test', 'dailyTest', 'insert_one', "103.6.254.48/")

            del new_mention_df, new_pol_tweets_df, user_hashtag_tweets_df, extend_politician_df

            print('{} daily analysis finished. Time used: {} mins'.format(datetime.strftime(start, '%b-%d-%Y'),
                                                                          (time.time() - start_time) / 60))
            gc.collect()

# if __name__ == '__main__':
#     f_tools = functional_tools.FunctionalTools()
#     start_time = time.time()
#     print('Start read politician data')
#     start_time = time.time()
#
#     # read politician data from mongoDB
#     politician_from_mongo = f_tools.find_data('capstone', 'politicianInfo')
#     politician_df = pd.DataFrame(list(politician_from_mongo))
#
#     # create daily timestamp
#     start_date_str = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
#     end_date_str = datetime.date.today().strftime('%Y-%m-%d')
#     # date_list = pd.date_range(start=start_date_str, end=end_date_str, freq='D')
#     date_list = pd.date_range(start='2019-04-13', end='2019-04-21', freq='D')
#
#     for i in range(len(date_list) - 1):
#         start = date_list[i]
#         end = date_list[i + 1]
#         result_dict = {}
#
#         # read daily data from mongoDB
#         pol_tweets_mongo = f_tools.find_mongo_by_date('capstone', 'restfulTweets', start, end)
#         pol_tweets_df = pd.DataFrame(list(pol_tweets_mongo))
#
#         user_tweets_mongo = f_tools.find_mongo_by_date('capstone', 'restfulByHashtag', start, end)
#         user_tweets_df = pd.DataFrame(list(user_tweets_mongo))
#
#         totalMention_from_mongo = f_tools.find_mongo_by_date('capstone', 'streamingMentionedCorrectDate', start, end)
#         totalMention_df = pd.DataFrame(list(totalMention_from_mongo))
#         user_tweets_df = user_tweets_df.append(totalMention_df, ignore_index=True)
#         print('Daily tweets loaded.')
#
#         daily_analysis = tweets_analysis.TweetsAnalysis(politician_df, totalMention_df, pol_tweets_df)
#
#         # ===daily statistical data analysis===
#         extend_politician_df = daily_analysis.features_concat()
#
#         # ===popular hashtags analysis===
#         pol_top_tags = daily_analysis.count_popular_hashtag(pol_tweets_df)
#         user_top_tags = daily_analysis.count_popular_hashtag(user_tweets_df)
#         result_dict['date'] = datetime.datetime.strftime(start, '%b-%d-%Y')
#         result_dict['data'] = {'Top_Tags_of_Politicians': pol_top_tags,
#                                'Top_Tags_of_Users': user_top_tags,
#                                'dailyPolitician': extend_politician_df.to_dict('records')}
#
#         # f_tools.save_data(result_dict, 'test', 'dailyHead', 'insert_one')
#         f_tools.save_data(result_dict, 'test', 'dailyTest', 'insert_one')
#
#         del pol_tweets_mongo, pol_tweets_df, user_tweets_mongo, user_tweets_df, totalMention_from_mongo, totalMention_df
#         del extend_politician_df
#         gc.collect()
#         print('{} daily analysis finished. Time used: {} mins'.format(datetime.datetime.strftime(start, '%b-%d-%Y'),
#                                                                       (time.time() - start_time) / 60))
