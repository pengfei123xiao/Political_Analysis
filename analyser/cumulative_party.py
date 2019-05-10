#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 9/05/2019 1:47 PM
# @Author  : Pengfei Xiao
# @FileName: cumulative_party.py
# @Software: PyCharm

import pandas as pd
import numpy as np
from datetime import datetime
import nltk
from nltk.corpus import stopwords
nltk.download('stopwords')
import time
from tweet_analyser import TweetAnalyser
import gc

gc.enable()


class CumulativeParty():
    def __init__(self, politician_df, cummulative_df, pol_tweets_df):
        self.politician_df = politician_df
        self.cumulative_df = cummulative_df
        self.pol_tweets_df = pol_tweets_df

    def reply_count(self, x):
        count = 0
        for _, s_list in self.cumulative_df['Mentioned_Screen_Name'].iteritems():
            if x in s_list:
                count += 1
        return count

    def sentiment_sum(self, x):
        count = 0
        for _, s_list in self.cumulative_df['Mentioned_Screen_Name'].iteritems():
            if x in s_list:
                count += self.cumulative_df['Content_Sentiment'].iloc[i]
        return count

    def sentiment_percent(self, x):
        pos, neu, neg = 0, 0, 0
        for _, s_list in self.cumulative_df['Mentioned_Screen_Name'].iteritems():
            if x in s_list:
                if self.cumulative_df['Content_Sentiment'].iloc[i] == 1:
                    pos += 1
                elif self.cumulative_df['Content_Sentiment'].iloc[i] == 0:
                    neu += 1
                else:
                    neg += 1
        return [pos, neu, neg]

    def word_frequency(self, screen_name):
        # tweets = self.pol_tweets_df[self.pol_tweets_df['Screen_Name'] == screen_name]['Tweets'].tolist()
        origin_pol_tweets_df = self.pol_tweets_df[~self.pol_tweets_df['Tweets'].str.contains('RT')]
        tweets = origin_pol_tweets_df[origin_pol_tweets_df['Screen_Name'] == screen_name]['Tweets'].tolist()
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

        """word cloud"""
        self.politician_df['Word_Cloud'] = self.politician_df['Screen_Name'].apply(
            lambda x: self.word_frequency(x))

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
    date_list = pd.date_range(start='2019-04-13', end='2019-04-21', freq='D', tz='Australia/Sydney')
    result_dict = {}
    for i in range(len(date_list) - 1):
        start = date_list[i]
        end = date_list[i + 1]

        # slicing dataframe via cumulative date
        cummulative_df = concat_df[concat_df['Date'] <= end]
        cummulative_df.reset_index(drop=True, inplace=True)

        # read cumulative politician tweets
        pol_tweets_from_mongo = analyser.find_mongo_by_date('capstone', 'restfulTweets', datetime(2019, 4, 13), end)
        pol_tweets_df = pd.DataFrame(list(pol_tweets_from_mongo))

        cumulative_pol = CumulativeParty(politician_df, cummulative_df, pol_tweets_df)
        extend_pol_tweets_df = cumulative_pol.features_concat()
        print('Features concat finished. Time used: %f mins' % ((time.time() - start_time) / 60))
        del pol_tweets_from_mongo, pol_tweets_df, cummulative_df
        gc.collect()
        result_dict[datetime.strftime(start, '%b-%d-%Y')] = extend_pol_tweets_df.to_dict('records')
        # analyser.save_data({datetime.strftime(start, '%b-%d-%Y'): extend_pol_tweets_df.to_dict('records')}, 'test',
        #                    'sumResult', 'insert_one')
    analyser.save_data(result_dict, 'test', 'sumResult1', 'insert_one')