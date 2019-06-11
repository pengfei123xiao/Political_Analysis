#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 9/05/2019 1:47 PM
# @Author  : Pengfei Xiao
# @FileName: tweets_analysis.py
# @Software: PyCharm

"""This file defines all functionality for tweets statistical analysis."""

import nltk
from nltk.corpus import stopwords

nltk.download('stopwords')
import pandas as pd
import sys

sys.path.append('..')
import collections, functools, operator
import gc
import logging

gc.enable()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler('tweets_analysis.log')
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


def mem_usage(pandas_obj):
    if isinstance(pandas_obj, pd.DataFrame):
        usage_b = pandas_obj.memory_usage(deep=True).sum()
    else:  # we assume if not a df it's a series
        usage_b = pandas_obj.memory_usage(deep=True)
    usage_mb = usage_b / 1024 ** 2  # convert bytes to megabytes
    return "{:03.2f} MB".format(usage_mb)


class TweetsAnalysis():
    def __init__(self, politician_df, mentioned_df, pol_tweets_df):
        self.politician_df = politician_df
        self.mentioned_df = mentioned_df
        self.pol_tweets_df = pol_tweets_df
        self.origin_pol_tweets_df = self.pol_tweets_df[~self.pol_tweets_df['Tweets'].str.contains('RT')]
        self.mentioned_df.info(memory_usage='deep')
        logger.info("Memory usage of new_mention_df: {}".format(mem_usage(self.mentioned_df)))

    def statistical_count(self, screen_name):
        """
        Mention and sentiment data calculation for every politician.
        :param screen_name: str
            Politician screen name.

        :return: list
            A list of mention and sentiment count results.
        """
        mentioned_count, head_pos, head_neu, head_neg = 0, 0, 0, 0
        mentioned_user_dic = {}
        self.mentioned_df.reset_index(drop=True, inplace=True)
        for index, s_list in self.mentioned_df['Mentioned_Screen_Name'].iteritems():
            if screen_name in s_list:
                mentioned_count += 1
                user_screen_name = self.mentioned_df['Screen_Name'].iloc[index]
                if user_screen_name in mentioned_user_dic:
                    mentioned_user_dic[user_screen_name][self.mentioned_df['Content_Sentiment'].iloc[index] + 1] += 1
                    # [neg, neu, pos]
                else:
                    mentioned_user_dic[user_screen_name] = [0, 0, 0]
                    mentioned_user_dic[user_screen_name][self.mentioned_df['Content_Sentiment'].iloc[index] + 1] += 1
        for i, v in mentioned_user_dic.items():
            if v[2] - v[0] > 0:  # if pos > neg
                head_pos += 1
            elif v[2] - v[0] == 0:  # if pos = neg
                head_neu += 1
            else:  # if pos < neg
                head_neg += 1
        return [mentioned_count, head_pos, head_neu, head_neg]

    def word_frequency(self, name, column):
        """
        Count word frequency to create a word cloud.
        :param name: str
            Screen name or party name.
        :param column: str
            Column name.

        :return: dict
            A dictionary contains top 50 words and their frequency.
        """
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

        # calculate frequency of word
        freq = nltk.FreqDist(clean_tokens)
        top_fifty = freq.most_common(50)
        result = []
        for item in top_fifty:
            result.append({'text': item[0], 'value': item[1]})
        return result

    def features_concat(self):
        """
        Attach all the calculated features to politician dataframe.

        :return: Dataframe
            A dataframe storing the politician info and statistical data.
        """
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
            lambda x: pos_pol_tweets_df.groupby(by='Screen_Name')['Content_Sentiment'].count()[x] if x in
                                                                                                     pos_pol_tweets_df[
                                                                                                         'Screen_Name'].tolist() else 0)
        self.politician_df['Pol_Sentiment_Neu'] = self.politician_df['Screen_Name'].apply(
            lambda x: neu_pol_tweets_df.groupby(by='Screen_Name')['Content_Sentiment'].count()[x] if x in
                                                                                                     neu_pol_tweets_df[
                                                                                                         'Screen_Name'].tolist() else 0)
        self.politician_df['Pol_Sentiment_Neg'] = self.politician_df['Screen_Name'].apply(
            lambda x: neg_pol_tweets_df.groupby(by='Screen_Name')['Content_Sentiment'].count()[x] if x in
                                                                                                     neg_pol_tweets_df[
                                                                                                         'Screen_Name'].tolist() else 0)
        # ===statistical count from reply tweets===
        self.politician_df['Statistical_Count'] = self.politician_df['Screen_Name'].apply(
            lambda x: self.statistical_count(x))

        # ===daily reply count===
        self.politician_df['Mentioned_Count'] = self.politician_df['Statistical_Count'].apply(lambda x: x[0])
        # ===sentiment distribution===
        self.politician_df['Sentiment_Pos'] = self.politician_df['Statistical_Count'].apply(lambda x: x[1])
        self.politician_df['Sentiment_Neu'] = self.politician_df['Statistical_Count'].apply(lambda x: x[2])
        self.politician_df['Sentiment_Neg'] = self.politician_df['Statistical_Count'].apply(lambda x: x[3])

        # ===state sentiment distribution===
        pos_mention_df = self.mentioned_df[self.mentioned_df['Content_Sentiment'] == 1]
        neu_mention_df = self.mentioned_df[self.mentioned_df['Content_Sentiment'] == 0]
        neg_mention_df = self.mentioned_df[self.mentioned_df['Content_Sentiment'] == -1]
        self.politician_df['State_Pos'] = \
            self.politician_df['Screen_Name'].apply(lambda x: [(pos_mention_df[
                                                                    pos_mention_df['Mentioned_Screen_Name'].astype(
                                                                        str).str.contains(x, na=False)].groupby(
                by='State')['Screen_Name'].nunique()).to_dict()])
        self.politician_df['State_Neu'] = \
            self.politician_df['Screen_Name'].apply(lambda x: [(neu_mention_df[
                                                                    neu_mention_df['Mentioned_Screen_Name'].astype(
                                                                        str).str.contains(x, na=False)].groupby(
                by='State')['Screen_Name'].nunique()).to_dict()])
        self.politician_df['State_Neg'] = \
            self.politician_df['Screen_Name'].apply(lambda x: [(neg_mention_df[
                                                                    neg_mention_df['Mentioned_Screen_Name'].astype(
                                                                        str).str.contains(x, na=False)].groupby(
                by='State')['Screen_Name'].nunique()).to_dict()])

        self.politician_df.drop(columns=['Statistical_Count'], inplace=True)

        # ===word cloud===
        self.politician_df['Word_Cloud'] = self.politician_df['Screen_Name'].apply(
            lambda x: self.word_frequency(x, 'Screen_Name'))
        return self.politician_df

    def state_sentiment_party(self, politician_df, party, col_name):
        """
        Sentiment analysis from different state.
        :param politician_df: dataframe
            A dataframe storing the politician info and statistical data.
        :param party: str
            The political party information.
        :param col_name: str
            The column name of the dataframe.

        :return:
        """
        state_sentiment = []
        state_list = ['New South Wales', 'Victoria', 'Queensland', 'South Australia', 'Western Australia',
                      'Northern Territory', 'Australian Capital Territory', 'Other Territories', 'Tasmania']
        for i, v in politician_df[politician_df['Party'] == party][col_name].iteritems():
            if len(v[0]) != 0:
                state_sentiment.extend(v)
        if len(state_sentiment) != 0:
            result = dict(functools.reduce(operator.add, map(collections.Counter, state_sentiment)))
            for state in state_list:
                if state not in result:
                    result[state] = 0
            return [result]
        else:
            return [{'Australian Capital Territory': 0,
                     'New South Wales': 0,
                     'Northern Territory': 0,
                     'Other Territories': 0,
                     'Queensland': 0,
                     'South Australia': 0,
                     'Tasmania': 0,
                     'Victoria': 0,
                     'Western Australia': 0}]
