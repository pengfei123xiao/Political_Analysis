#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 8/05/2019 8:58 PM
# @Author  : Pengfei Xiao
# @FileName: cumulative_politician.py
# @Software: PyCharm

import pandas as pd
import numpy as np
from datetime import datetime
import nltk
from nltk.corpus import stopwords
# nltk.download('stopwords')
import time
import swifter
from tweet_analyser import TweetAnalyser
import gc

gc.enable()


class CumulativePolitician():
    def __init__(self, politician_df1, cummulative_df1, pol_tweets_df1):
        self.politician_df = politician_df1
        self.cumulative_df = cummulative_df1
        self.pol_tweets_df = pol_tweets_df1
        self.origin_pol_tweets_df = self.pol_tweets_df[~self.pol_tweets_df['Tweets'].str.contains('RT')]

    def statistical_count(self, screen_name):
        reply_count, sen_sum = 0, 0
        pos, neu, neg = 0, 0, 0
        for index, s_list in self.cumulative_df['Mentioned_Screen_Name'].iteritems():
            if screen_name in s_list:
                reply_count += 1
                sen_sum += self.cumulative_df['Content_Sentiment'].iloc[index]
                if self.cumulative_df['Content_Sentiment'].iloc[index] == 1:
                    pos += 1
                elif self.cumulative_df['Content_Sentiment'].iloc[index] == 0:
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
            lambda x: (self.pol_tweets_df.Screen_Name == x).sum() if x in self.pol_tweets_df[
                'Screen_Name'].tolist() else 0)
        self.politician_df['Retweets_Count'] = self.politician_df['Screen_Name'].apply(
            lambda x: self.origin_pol_tweets_df.groupby(by='Screen_Name')['Retweets'].sum()[x] if x in
                                                                                                  self.origin_pol_tweets_df[
                                                                                                      'Screen_Name'].tolist() else 0)
        self.politician_df['Likes_Count'] = self.politician_df['Screen_Name'].apply(
            lambda x: self.pol_tweets_df.groupby(by='Screen_Name')['Likes'].sum()[x] if x in self.pol_tweets_df[
                'Screen_Name'].tolist() else 0)

        """cumulative statistical count"""
        self.politician_df['Statistical_Count'] = self.politician_df['Screen_Name'].apply(
            lambda x: self.statistical_count(x))

        # ===daily reply count===
        self.politician_df['Reply_Count'] = self.politician_df['Statistical_Count'].apply(lambda x: x[0])
        # np.array([v[0] for _, v in self.politician_df['Statistical_Count'].iteritems()])
        # ===sentiment sum count===
        self.politician_df['Sentiment_Sum'] = self.politician_df['Statistical_Count'].apply(lambda x: x[1])
        # np.array([v[1] for _, v in self.politician_df['Statistical_Count'].iteritems()])
        # ===sentiment distribution===
        self.politician_df['Sentiment_Pos'] = self.politician_df['Statistical_Count'].apply(lambda x: x[2])
        self.politician_df['Sentiment_Neu'] = self.politician_df['Statistical_Count'].apply(lambda x: x[3])
        self.politician_df['Sentiment_Neg'] = self.politician_df['Statistical_Count'].apply(lambda x: x[4])
        self.politician_df.drop(columns=['Statistical_Count'], inplace=True)

        """word cloud"""
        self.politician_df['Word_Cloud'] = self.politician_df['Screen_Name'].apply(
            lambda x: self.word_frequency(x, 'Screen_Name'))
        return self.politician_df


if __name__ == '__main__':
    analyser = TweetAnalyser()
    print('Start read data')
    start_time = time.time()
    # read data from mongoDB
    politician_from_mongo = analyser.find_data('capstone', 'politicianInfo')
    politician_df = pd.DataFrame(list(politician_from_mongo))
    # restMention_from_mongo = analyser.find_data('capstone', 'restfulMentioned')
    # streamMention_from_mongo = analyser.find_data('capstone', 'streamingMentioned')
    # restMention_df = pd.DataFrame(list(restMention_from_mongo))
    # streamMention_df = pd.DataFrame(list(streamMention_from_mongo))

    # streaming 'Date' conversion
    # streamMention_df['Date'] = streamMention_df['Date'].apply(lambda x: datetime.strptime(x, '%a %b %d %H:%M:%S %z %Y'))

    # convert to tz-aware timestamp for comparison
    # streamMention_df['Date'] = streamMention_df['Date'].dt.tz_convert('Australia/Sydney')
    # restMention_df['Date'] = restMention_df['Date'].dt.tz_localize('Australia/Sydney')

    # concat mentioned data and remove duplicates
    # concat_df = pd.concat([restMention_df, streamMention_df], sort=False)
    # concat_df.drop_duplicates(subset=['ID'], keep='first', inplace=True)
    # concat_df.reset_index(drop=True, inplace=True)
    # print('Mentioned concat finished. Time used: %f mins' % ((time.time() - start_time) / 60))

    # del politician_from_mongo, restMention_from_mongo, streamMention_from_mongo, restMention_df, streamMention_df
    del politician_from_mongo
    gc.collect()

    # create daily timestamp
    date_list = pd.date_range(start='2019-04-13', end='2019-4-15', freq='D', tz='Australia/Sydney')
    result_dict = {}
    pol_result = []
    party_result = []
    for i in range(len(date_list) - 1):
        start = date_list[i]
        end = date_list[i + 1]

        # slicing dataframe via cumulative date
        totalMention_from_mongo = analyser.find_mongo_by_date('capstone', 'streamingMentionedCorrectDate',
                                                              datetime(2019, 4, 13), end)
        totalMention_df = pd.DataFrame(list(totalMention_from_mongo))
        print('Mentioned data loaded. Time used: %f mins' % ((time.time() - start_time) / 60))
        # cummulative_df = concat_df[concat_df['Date'] <= end]
        # cummulative_df.reset_index(drop=True, inplace=True)

        # read cumulative politician tweets
        pol_tweets_from_mongo = analyser.find_mongo_by_date('capstone', 'restfulTweets', datetime(2019, 4, 13), end)
        pol_tweets_df = pd.DataFrame(list(pol_tweets_from_mongo))

        # cumulative_pol = CumulativePolitician(politician_df, cummulative_df, pol_tweets_df)
        cumulative_pol = CumulativePolitician(politician_df, totalMention_df, pol_tweets_df)
        extend_politician_df = cumulative_pol.features_concat()
        print('{} features concat finished. Time used: {} mins'.format(datetime.strftime(start, '%b %d %Y'),
                                                                       (time.time() - start_time) / 60))
        del pol_tweets_from_mongo, totalMention_from_mongo, totalMention_df, pol_tweets_df
        gc.collect()
        # result_dict[datetime.strftime(start, '%b-%d-%Y')] = extend_politician_df.to_dict('records')
        # result.append({'date': datetime.strftime(start, '%b-%d-%Y'), 'data': extend_politician_df.to_dict('records')})
        pol_result.append(
            {'date': datetime.strftime(start, '%b-%d-%Y'), 'data': extend_politician_df.to_dict('records')})

        # ===cumulative party data===
        temp_df = extend_politician_df.copy()
        temp_df.drop(columns=['Avatar', 'Create_Time', 'Description', 'Electoral_District', 'ID',
                              'Location', 'Name', 'Screen_Name', 'State', '_id'], inplace=True)
        sum_party_df = temp_df.groupby(by='Party').agg('sum')
        sum_party_df['Word_Cloud'] = sum_party_df.index.map(lambda x: cumulative_pol.word_frequency(x, 'Party'))
        sum_party_df['Party'] = sum_party_df.index
        party_result.append(
            {'date': datetime.strftime(start, '%b-%d-%Y'), 'data': sum_party_df.to_dict('records')})

        del extend_politician_df, temp_df, sum_party_df
        gc.collect()

    result_dict['sumPolitician'] = pol_result
    result_dict['sumParty'] = party_result
    # analyser.save_data(result, 'test', 'test', 'insert_many')
    analyser.save_data(result_dict, 'test', 'sum', 'insert_one')
