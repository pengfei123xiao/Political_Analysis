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
from tweet_analyser import TweetAnalyser
import gc

gc.enable()


class DailyAnalysis():
    def __init__(self, politician_df1, daily_df1, pol_tweets_df1):
        self.politician_df = politician_df1
        self.daily_df = daily_df1
        self.pol_tweets_df = pol_tweets_df1

    def reply_count(self, x):
        count = 0
        for _, s_list in self.daily_df['Mentioned_Screen_Name'].iteritems():
            if x in s_list:
                count += 1
        return count

    def sentiment_sum(self, x):
        count = 0
        for _, s_list in self.daily_df['Mentioned_Screen_Name'].iteritems():
            if x in s_list:
                count += self.daily_df['Content_Sentiment'].iloc[i]
        return count

    def sentiment_percent(self, x):
        pos, neu, neg = 0, 0, 0
        for _, s_list in self.daily_df['Mentioned_Screen_Name'].iteritems():
            if x in s_list:
                if self.daily_df['Content_Sentiment'].iloc[i] == 1:
                    pos += 1
                elif self.daily_df['Content_Sentiment'].iloc[i] == 0:
                    neu += 1
                else:
                    neg += 1
        return [pos, neu, neg]

    def features_concat(self):
        # print(self.pol_tweets_df['Tweets'].iloc[:3])
        origin_pol_tweets_df = self.pol_tweets_df[~self.pol_tweets_df['Tweets'].str.contains('RT')]
        self.politician_df['Tweets_Count'] = self.politician_df['Screen_Name'].apply(
            lambda x: (self.pol_tweets_df.Screen_Name == x).sum() if x in self.pol_tweets_df[
                'Screen_Name'].tolist() else 0)
        self.politician_df['Retweets_Count'] = self.politician_df['Screen_Name'].apply(
            lambda x: origin_pol_tweets_df.groupby(by='Screen_Name')['Retweets'].sum()[x] if x in origin_pol_tweets_df[
                'Screen_Name'].tolist() else 0)
        self.politician_df['Likes_Count'] = self.politician_df['Screen_Name'].apply(
            lambda x: self.pol_tweets_df.groupby(by='Screen_Name')['Likes'].sum()[x] if x in self.pol_tweets_df[
                'Screen_Name'].tolist() else 0)

        """daily reply count"""
        self.politician_df['Reply_Count'] = self.politician_df['Screen_Name'].apply(lambda x: self.reply_count(x))

        """daily sentiment sum"""
        self.politician_df['Sentiment_Sum'] = self.politician_df['Screen_Name'].apply(lambda x: self.sentiment_sum(x))

        """daily sentiment percentage"""
        self.politician_df['Sentiment_Distribution'] = self.politician_df['Screen_Name'].apply(
            lambda x: self.sentiment_percent(x))
        return self.politician_df


if __name__ == '__main__':
    analyser = TweetAnalyser()
    print('Start read data')
    start_time = time.time()
    # read data from mongoDB
    politician_from_mongo = analyser.find_data('capstone', 'politicianInfo')
    politician_df = pd.DataFrame(list(politician_from_mongo))
    restMention_from_mongo = analyser.find_data('capstone', 'restfulMentioned')
    streamMention_from_mongo = analyser.find_data('capstone', 'streamingMentioned')
    restMention_df = pd.DataFrame(list(restMention_from_mongo))
    streamMention_df = pd.DataFrame(list(streamMention_from_mongo))

    # streaming 'Date' conversion
    streamMention_df['Date'] = streamMention_df['Date'].apply(lambda x: datetime.strptime(x, '%a %b %d %H:%M:%S %z %Y'))

    # convert to tz-aware timestamp for comparison
    streamMention_df['Date'] = streamMention_df['Date'].dt.tz_convert('Australia/Sydney')
    restMention_df['Date'] = restMention_df['Date'].dt.tz_localize('Australia/Sydney')

    # concat mentioned data and remove duplicates
    concat_df = pd.concat([restMention_df, streamMention_df], sort=False)
    concat_df.drop_duplicates(subset=['ID'], keep='first', inplace=True)
    concat_df.reset_index(drop=True, inplace=True)
    print('Mentioned concat finished. Time used: %f mins' % ((time.time() - start_time) / 60))

    del politician_from_mongo, restMention_from_mongo, streamMention_from_mongo, restMention_df, streamMention_df
    gc.collect()

    # create daily timestamp
    date_list = pd.date_range(start='2019-04-14', end='2019-04-21', freq='D', tz='Australia/Sydney')
    result_dict = {}
    for i in range(len(date_list) - 1):
        start = date_list[i]
        end = date_list[i + 1]

        # slicing dataframe via daily time
        daily_df = concat_df[(start <= concat_df['Date']) & (concat_df['Date'] <= end)]
        daily_df.reset_index(drop=True, inplace=True)

        # read daily politician tweets
        pol_tweets_from_mongo = analyser.find_mongo_by_date('capstone', 'restfulTweets', start, end)
        pol_tweets_df = pd.DataFrame(list(pol_tweets_from_mongo))
        # origin_pol_tweets_df = pol_tweets_df[~pol_tweets_df['Tweets'].str.contains('RT')]

        daily_analysis = DailyAnalysis(politician_df, daily_df, pol_tweets_df)
        extend_pol_tweets_df = daily_analysis.features_concat()
        print('Features concat finished. Time used: %f mins' % ((time.time() - start_time) / 60))
        del pol_tweets_from_mongo, pol_tweets_df, daily_df
        gc.collect()
        result_dict[datetime.strftime(start, '%b-%d-%Y')] = extend_pol_tweets_df.to_dict('records')
        # analyser.save_data({datetime.strftime(start, '%b-%d-%Y'): extend_pol_tweets_df.to_dict('records')}, 'test',
        #                    'dailyResult', 'insert_one')
    analyser.save_data(result_dict, 'test', 'dailyResult', 'insert_one')
