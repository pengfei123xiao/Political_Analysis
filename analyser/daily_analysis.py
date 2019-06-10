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
        state_list = ['New South Wales', 'Victoria', 'Queensland', 'South Australia', 'Western Australia',
                      'Northern Territory', 'Australian Capital Territory', 'Other Territories', 'Tasmania']
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
            user_hashtag_tweets_df = user_hashtag_tweets_df.append(new_mention_df, ignore_index=True,sort=False)
            print('Daily tweets loaded.')

            daily_analysis = tweets_analysis.TweetsAnalysis(self.politician_df, new_mention_df, new_pol_tweets_df)

            # ===daily statistical data analysis===
            extend_politician_df = daily_analysis.features_concat()
            for _, v in extend_politician_df.iterrows():
                for state in state_list:
                    if state not in v['State_Pos'][0]:
                        v['State_Pos'][0][state] = 0
                    if state not in v['State_Neu'][0]:
                        v['State_Neu'][0][state] = 0
                    if state not in v['State_Neg'][0]:
                        v['State_Neg'][0][state] = 0

            # ===daily party analysis===
            daily_party_df = extend_politician_df.groupby(by='Party').agg('sum')
            daily_party_df['Word_Cloud'] = daily_party_df.index.map(lambda x: daily_analysis.word_frequency(x, 'Party'))
            daily_party_df['Party'] = daily_party_df.index
            # state sentiment distribution
            daily_party_df['State_Pos'] = daily_party_df['Party'].apply(
                lambda x: daily_analysis.state_sentiment_party(extend_politician_df, x, 'State_Pos'))
            daily_party_df['State_Neu'] = daily_party_df['Party'].apply(
                lambda x: daily_analysis.state_sentiment_party(extend_politician_df, x, 'State_Neu'))
            daily_party_df['State_Neg'] = daily_party_df['Party'].apply(
                lambda x: daily_analysis.state_sentiment_party(extend_politician_df, x, 'State_Neg'))

            # ===popular hashtags analysis===
            pol_top_tags = self.f_tools.count_popular_hashtag(new_pol_tweets_df)
            user_top_tags = self.f_tools.count_popular_hashtag(user_hashtag_tweets_df)
            result_dict['date'] = datetime.strftime(end, '%b-%d-%Y')
            result_dict['data'] = {'Top_Tags_of_Politicians': pol_top_tags,
                                   'Top_Tags_of_Users': user_top_tags,
                                   'dailyPolitician': extend_politician_df.to_dict('records'),
                                   'dailyParty': daily_party_df.to_dict('records')
                                   }

            # f_tools.save_data(result_dict, 'test', 'dailyHead', 'insert_one')
            self.f_tools.save_data(result_dict, 'test', 'dailyTest', 'insert_one', "103.6.254.48/")

            del new_mention_df, new_pol_tweets_df, user_hashtag_tweets_df, extend_politician_df
            gc.collect()

            print('{} daily analysis finished. Time used: {} mins'.format(datetime.strftime(start, '%b-%d-%Y'),
                                                                          (time.time() - start_time) / 60))
