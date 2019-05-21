#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 9/05/2019 1:47 PM
# @Author  : Pengfei Xiao
# @FileName: tweets_analysis.py
# @Software: PyCharm

import pandas as pd
import numpy as np
from datetime import datetime

import nltk
from nltk.corpus import stopwords
# nltk.download('stopwords')
import sys

sys.path.append('..')
# from analyser.functional_tools import FunctionalTools
import gc

gc.enable()


class TweetsAnalysis():
    """
    This class is used to analysis politician and party statistical data.
    Attributes:
        :param politician_df:
        :param mentioned_df:
        :param pol_tweets_df:
    """

    def __init__(self, politician_df, mentioned_df, pol_tweets_df):
        self.politician_df = politician_df
        self.mentioned_df = mentioned_df
        self.pol_tweets_df = pol_tweets_df
        self.origin_pol_tweets_df = self.pol_tweets_df[~self.pol_tweets_df['Tweets'].str.contains('RT')]

    # def statistical_count(self, screen_name):
    #     mentioned_count, reply_count, sen_sum = 0, 0, 0
    #     pos, neu, neg = 0, 0, 0
    #     for index, s_list in self.mentioned_df['Mentioned_Screen_Name'].iteritems():
    #         if screen_name in s_list:
    #             mentioned_count += 1
    #             sen_sum += self.mentioned_df['Content_Sentiment'].iloc[index]
    #             if self.mentioned_df['In_Reply_to_Screen_Name'].iloc[index] is not None and screen_name in \
    #                     self.mentioned_df['In_Reply_to_Screen_Name'].iloc[index]:
    #                 reply_count += 1
    #             if self.mentioned_df['Content_Sentiment'].iloc[index] == 1:
    #                 pos += 1
    #             elif self.mentioned_df['Content_Sentiment'].iloc[index] == 0:
    #                 neu += 1
    #             else:
    #                 neg += 1
    #     mentioned_count = mentioned_count - reply_count
    #     return [mentioned_count, sen_sum, pos, neu, neg, reply_count]

    def statistical_count(self, screen_name):
        mentioned_count, reply_count, sen_sum = 0, 0, 0
        pos, neu, neg = 0, 0, 0
        pos1, neu1, neg1 = 0, 0, 0
        mentioned_user_dic = {}
        for index, s_list in self.mentioned_df['Mentioned_Screen_Name'].iteritems():
            if screen_name in s_list:
                mentioned_count += 1
                # if self.mentioned_df['Content_Sentiment'].iloc[index] == 1:
                #     pos += 1
                # elif self.mentioned_df['Content_Sentiment'].iloc[index] == 0:
                #     neu += 1
                # else:
                #     neg += 1
                user_screen_name = self.mentioned_df['Screen_Name'].iloc[index]
                if user_screen_name in mentioned_user_dic:
                    mentioned_user_dic[user_screen_name][self.mentioned_df['Content_Sentiment'].iloc[index] + 1] += 1
                    # [neg, neu, pos]
                else:
                    mentioned_user_dic[user_screen_name] = [0, 0, 0]
                    mentioned_user_dic[user_screen_name][self.mentioned_df['Content_Sentiment'].iloc[index] + 1] += 1
        for i, v in mentioned_user_dic.items():
            if v[2] - v[0] > 0:
                pos1 += 1
            elif v[2] - v[0] == 0:
                neu1 += 1
            else:
                neg1 += 1
        mentioned_count = mentioned_count - reply_count
        return [mentioned_count, pos1, neu1, neg1]  # , pos, neu, neg]

    def word_frequency(self, name, column):
        """
        :param name: str
            Screen name or party name.
        :param column: str
            Column name.
        :return:
        """
        # tweets = self.pol_tweets_df[self.pol_tweets_df['Screen_Name'] == screen_name]['Tweets'].tolist()
        tweets = self.origin_pol_tweets_df[self.origin_pol_tweets_df[column] == name]['Tweets'].tolist()
        sr = stopwords.words('english')
        sr.extend(['-', '&amp;', '|', '…'])
        special_words = ['http', '#', '@']
        tokens = []
        for tweet in tweets:
            tokens.extend(tweet.lower().split())

        # clean stop words and special words
        clean_tokens = []
        for token in tokens:
            if not any(x in token for x in special_words) and token not in sr:
                clean_tokens.append(token)

        # calculate frequence of word
        freq = nltk.FreqDist(clean_tokens)
        top_fifty = freq.most_common(50)

        result = []
        for item in top_fifty:
            result.append({'text': item[0], 'value': item[1]})
        return result

    def features_concat(self):
        self.politician_df['Tweets_Count'] = self.politician_df['Screen_Name'].apply(
            lambda x: (self.pol_tweets_df['Screen_Name'] == x).sum() if x in self.pol_tweets_df[
                'Screen_Name'].tolist() else 0)
        self.politician_df['Retweets_Count'] = self.politician_df['Screen_Name'].apply(
            lambda x: self.origin_pol_tweets_df.groupby(by='Screen_Name')['Retweets'].sum()[x] if x in
                                                                                                  self.origin_pol_tweets_df[
                                                                                                      'Screen_Name'].tolist() else 0)
        self.politician_df['Likes_Count'] = self.politician_df['Screen_Name'].apply(
            lambda x: self.pol_tweets_df.groupby(by='Screen_Name')['Likes'].sum()[x] if x in self.pol_tweets_df[
                'Screen_Name'].tolist() else 0)
        pos_pol_tweets_df = self.pol_tweets_df[self.pol_tweets_df['Content_Sentiment'] == 1]
        neu_pol_tweets_df = self.pol_tweets_df[self.pol_tweets_df['Content_Sentiment'] == 0]
        neg_pol_tweets_df = self.pol_tweets_df[self.pol_tweets_df['Content_Sentiment'] == -1]
        self.politician_df['Pol_Sentiment_Pos'] = self.politician_df['Screen_Name'].apply(
            lambda x: pos_pol_tweets_df.groupby(by='Screen_Name')['Content_Sentiment'].sum()[x] if x in
                                                                                                   pos_pol_tweets_df[
                                                                                                       'Screen_Name'].tolist() else 0)
        self.politician_df['Pol_Sentiment_Neu'] = self.politician_df['Screen_Name'].apply(
            lambda x: neu_pol_tweets_df.groupby(by='Screen_Name')['Content_Sentiment'].sum()[x] if x in
                                                                                                   neu_pol_tweets_df[
                                                                                                       'Screen_Name'].tolist() else 0)
        self.politician_df['Pol_Sentiment_Neg'] = self.politician_df['Screen_Name'].apply(
            lambda x: neg_pol_tweets_df.groupby(by='Screen_Name')['Content_Sentiment'].sum()[x] if x in
                                                                                                   neg_pol_tweets_df[
                                                                                                       'Screen_Name'].tolist() else 0)

        """statistical count from reply tweets"""
        self.politician_df['Statistical_Count'] = self.politician_df['Screen_Name'].apply(
            lambda x: self.statistical_count(x))

        # ===daily reply count===
        self.politician_df['Mentioned_Count'] = self.politician_df['Statistical_Count'].apply(lambda x: x[0])
        # self.politician_df['Reply_Count'] = self.politician_df['Statistical_Count'].apply(lambda x: x[5])
        # ===sentiment sum count===
        # self.politician_df['Sentiment_Sum'] = self.politician_df['Statistical_Count'].apply(lambda x: x[1])
        # ===sentiment distribution===
        self.politician_df['Sentiment_Pos'] = self.politician_df['Statistical_Count'].apply(lambda x: x[1])
        self.politician_df['Sentiment_Neu'] = self.politician_df['Statistical_Count'].apply(lambda x: x[2])
        self.politician_df['Sentiment_Neg'] = self.politician_df['Statistical_Count'].apply(lambda x: x[3])
        # self.politician_df['Sentiment_Pos_raw'] = self.politician_df['Statistical_Count'].apply(lambda x: x[4])
        # self.politician_df['Sentiment_Neu_raw'] = self.politician_df['Statistical_Count'].apply(lambda x: x[5])
        # self.politician_df['Sentiment_Neg_raw'] = self.politician_df['Statistical_Count'].apply(lambda x: x[6])
        self.politician_df.drop(columns=['Statistical_Count'], inplace=True)

        """word cloud"""
        self.politician_df['Word_Cloud'] = self.politician_df['Screen_Name'].apply(
            lambda x: self.word_frequency(x, 'Screen_Name'))
        return self.politician_df
