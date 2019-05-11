#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 9/05/2019 1:47 PM
# @Author  : Pengfei Xiao
# @FileName: tweets_analysis.py
# @Software: PyCharm

import pandas as pd
import numpy as np
from datetime import datetime
from collections import Counter
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

    def statistical_count(self, screen_name):
        reply_count, sen_sum = 0, 0
        pos, neu, neg = 0, 0, 0
        for index, s_list in self.mentioned_df['Mentioned_Screen_Name'].iteritems():
            if screen_name in s_list:
                reply_count += 1
                sen_sum += self.mentioned_df['Content_Sentiment'].iloc[index]
                if self.mentioned_df['Content_Sentiment'].iloc[index] == 1:
                    pos += 1
                elif self.mentioned_df['Content_Sentiment'].iloc[index] == 0:
                    neu += 1
                else:
                    neg += 1
        return [reply_count, sen_sum, pos, neu, neg]

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
        self.politician_df['Pol_Sentiment_Sum'] = self.politician_df['Screen_Name'].apply(
            lambda x: self.pol_tweets_df.groupby(by='Screen_Name')['Content_Sentiment'].sum()[x] if x in
                                                                                                    self.pol_tweets_df[
                                                                                                        'Screen_Name'].tolist() else 0)

        """statistical count from reply tweets"""
        self.politician_df['Statistical_Count'] = self.politician_df['Screen_Name'].apply(
            lambda x: self.statistical_count(x))

        # ===daily reply count===
        self.politician_df['Reply_Count'] = self.politician_df['Statistical_Count'].apply(lambda x: x[0])
        # ===sentiment sum count===
        self.politician_df['Sentiment_Sum'] = self.politician_df['Statistical_Count'].apply(lambda x: x[1])
        # ===sentiment distribution===
        self.politician_df['Sentiment_Pos'] = self.politician_df['Statistical_Count'].apply(lambda x: x[2])
        self.politician_df['Sentiment_Neu'] = self.politician_df['Statistical_Count'].apply(lambda x: x[3])
        self.politician_df['Sentiment_Neg'] = self.politician_df['Statistical_Count'].apply(lambda x: x[4])
        self.politician_df.drop(columns=['Statistical_Count'], inplace=True)

        """word cloud"""
        self.politician_df['Word_Cloud'] = self.politician_df['Screen_Name'].apply(
            lambda x: self.word_frequency(x, 'Screen_Name'))
        return self.politician_df

    def count_popular_hashtag(self, tweets_df):
        #  tweet_with_tag_df = self.tweets_df[self.tweets_df['Hashtags'].astype(str) != '[]'].copy()
        tweet_with_tag_df = tweets_df[tweets_df['Hashtags'].astype(str) != '[]'].copy()
        tag_list = []
        for _, v in tweet_with_tag_df['Hashtags'].iteritems():
            tag_list.extend(v)
        tag_arr = np.empty([len(tag_list)], dtype=object)
        flag = 0
        for item in tag_list:
            tag_arr[flag] = item.lower()
            flag += 1
        tag_count = Counter(tag_arr)
        top_tags = tag_count.most_common(6)
        return top_tags
