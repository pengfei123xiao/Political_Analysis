#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 30/03/2019 7:13 PM
# @Author  : Pengfei Xiao
# @FileName: functional_tools.py
# @Software: PyCharm

import gc
import json
import re
from collections import Counter
from datetime import datetime

import emoji
import numpy as np
import pandas as pd
from pymongo import MongoClient, UpdateOne
from pytz import timezone
from textblob import TextBlob

gc.enable()


class FunctionalTools():
    """
    Functionality for analyzing and categorizing content from tweets.
    """

    def timezone_convert_streaming(self, utc_str):
        au_tz = timezone('Australia/Sydney')
        au_dt = datetime.strptime(utc_str, '%a %b %d %H:%M:%S %z %Y').astimezone(au_tz)
        return au_dt

    def timezone_convert(self, utc_dt):
        au_tz = timezone('Australia/Sydney')
        fmt = '%Y-%m-%d %H:%M:%S %Z%z'
        au_dt = utc_dt.astimezone(au_tz)
        au_dt.strftime(fmt)
        return au_dt

    def clean_tweet(self, tweet):
        tweet = emoji.demojize(tweet)  # interpret emoji
        return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", tweet).split())

    def analyze_sentiment(self, tweet):
        analysis = TextBlob(self.clean_tweet(tweet))
        if analysis.sentiment.polarity > 0:
            return 1
        elif analysis.sentiment.polarity == 0:
            return 0
        else:
            return -1

    def filter_tweet_streaming(self, raw_tweets_str):
        required_data = []
        for raw_tweet_str in raw_tweets_str:
            raw_tweet = json.loads(raw_tweet_str)
            if 'limit' not in raw_tweet:
                mentioned_screen_name_list = [user['screen_name'] for user in raw_tweet['entities']['user_mentions']]
                # print(1)
                hashtags = []

                if 'extended_tweet' in raw_tweet:
                    print("extended_tweets")
                    content = raw_tweet['extended_tweet']['full_text']
                    content_sentiment = self.analyze_sentiment(content)
                    hashtags.extend([item['text'] for item in raw_tweet['extended_tweet']['entities']['hashtags']])
                else:
                    content = raw_tweet['text']
                    content_sentiment = self.analyze_sentiment(content)
                    hashtags.extend([item['text'] for item in raw_tweet['entities']['hashtags']])

                if 'retweeted_status' in raw_tweet:
                    if 'extended_tweet' in raw_tweet['retweeted_status']:
                        text = raw_tweet['retweeted_status']['extended_tweet']['full_text']
                    else:
                        text = raw_tweet['retweeted_status']['text']
                    content = content + 'RT @' + raw_tweet['retweeted_status']['user']['screen_name'] + ': ' + text
                    # hashtags_retweeted =
                    hashtags.extend([item['text'] for item in raw_tweet['retweeted_status']['entities']['hashtags']])

                hashtags = list(set(hashtags))
                required_data.append({'ID': raw_tweet['id_str'], 'Screen_Name': raw_tweet['user']['screen_name'],
                                      'Date': self.timezone_convert_streaming(raw_tweet['created_at']),
                                      'Tweets': content, 'Content_Sentiment': content_sentiment,
                                      'Length': len(raw_tweet['text']), 'Language': raw_tweet['lang'],
                                      'Likes': raw_tweet['favorite_count'],
                                      'Retweets': raw_tweet['retweet_count'],
                                      'Mentioned_Screen_Name': mentioned_screen_name_list,
                                      'In_Reply_to_Screen_Name': raw_tweet['in_reply_to_screen_name'],
                                      'In_Reply_to_Status_id': raw_tweet['in_reply_to_status_id_str'],
                                      'Hashtags': hashtags, 'Location': raw_tweet['user']['location'],
                                      'Coordinates': raw_tweet['coordinates'], 'Source': raw_tweet['source']})
        return required_data

    def tweets_to_dataframe(self, raw_tweets):
        """
        Convert raw data into structured data.
        :param raw_tweets: Status
            Status data from twitter API.

        :return: dataframe
            Structured dataframe.
        """
        df = pd.DataFrame()
        contents = []
        tags = []
        mentioned = []
        df['ID'] = np.array([tweet.id_str for tweet in raw_tweets])
        df['Screen_Name'] = np.array([tweet.user.screen_name for tweet in raw_tweets])
        df['Date'] = np.array([tweet.created_at for tweet in raw_tweets])
        for tweet in raw_tweets:
            tags_single = []
            mentioned_single = [user['screen_name'] for user in tweet.entities['user_mentions']]
            mentioned.append(mentioned_single)
            if 'retweeted_status' in tweet._json:
                contents.append(
                    'RT @' + tweet._json['retweeted_status']['user']['screen_name'] + ': ' +
                    tweet._json['retweeted_status']['full_text'])
                tags_single = [item['text'] for item in tweet._json['retweeted_status']['entities']['hashtags']]
            else:
                contents.append(tweet.full_text)
            tags_single.extend([item['text'] for item in tweet.entities['hashtags']])
            tags_single = list(set(tags_single))
            tags.append(tags_single)
        df['Tweets'] = contents
        df['Hashtags'] = tags  # name modified
        df['Mentioned_Screen_Name'] = mentioned
        df['Content_Sentiment'] = np.array([self.analyze_sentiment(tweet.full_text) for tweet in raw_tweets])
        df['Length'] = np.array([len(tweet.full_text) for tweet in raw_tweets])
        df['Language'] = np.array([tweet.lang for tweet in raw_tweets])
        df['Likes'] = np.array([tweet.favorite_count for tweet in raw_tweets])
        df['Retweets'] = np.array([tweet.retweet_count for tweet in raw_tweets])
        df['In_Reply_to_Status_id'] = np.array([tweet.in_reply_to_status_id_str for tweet in raw_tweets])
        df['In_Reply_to_Screen_Name'] = np.array([tweet.in_reply_to_screen_name for tweet in raw_tweets])
        df['Location'] = np.array([tweet.user.location for tweet in raw_tweets])
        df['Coordinates'] = np.array([tweet.coordinates for tweet in raw_tweets])
        df['Source'] = np.array([tweet.source for tweet in raw_tweets])
        return df

    def pol_tweets_to_dataframe(self, raw_tweets, state_name, electorate_name, party_name):
        """
        Convert raw data into structured data.
        :param raw_tweets: Status
            Status data from twitter API.
        :param state_name: str
            State name of a politician.
        :param electorate_name: str
            Electoral District name of a politician.
        :param party_name: str
            Party name of a politician.

        :return: dataframe
            Structured dataframe.
        """
        df = pd.DataFrame()
        contents = []
        tags = []
        mentioned = []
        df['ID'] = np.array([tweet.id_str for tweet in raw_tweets])
        df['Screen_Name'] = np.array([tweet.user.screen_name for tweet in raw_tweets])
        df['Date'] = np.array([tweet.created_at for tweet in raw_tweets])
        for tweet in raw_tweets:
            tags_single = []
            mentioned_single = [user['screen_name'] for user in tweet.entities['user_mentions']]
            mentioned.append(mentioned_single)
            if 'retweeted_status' in tweet._json:
                contents.append(
                    'RT @' + tweet._json['retweeted_status']['user']['screen_name'] + ': ' +
                    tweet._json['retweeted_status']['full_text'])
                tags_single = [item['text'] for item in tweet._json['retweeted_status']['entities']['hashtags']]
            else:
                contents.append(tweet.full_text)
            tags_single.extend([item['text'] for item in tweet.entities['hashtags']])
            tags_single = list(set(tags_single))
            tags.append(tags_single)
        df['Tweets'] = contents
        df['Hashtags'] = tags  # name modified
        df['Mentioned_Screen_Name'] = mentioned
        df['Content_Sentiment'] = np.array([self.analyze_sentiment(tweet.full_text) for tweet in raw_tweets])
        df['Length'] = np.array([len(tweet.full_text) for tweet in raw_tweets])
        df['Language'] = np.array([tweet.lang for tweet in raw_tweets])
        df['Likes'] = np.array([tweet.favorite_count for tweet in raw_tweets])
        df['Retweets'] = np.array([tweet.retweet_count for tweet in raw_tweets])
        df['In_Reply_to_Status_id'] = np.array([tweet.in_reply_to_status_id_str for tweet in raw_tweets])
        df['In_Reply_to_Screen_Name'] = np.array([tweet.in_reply_to_screen_name for tweet in raw_tweets])
        df['Location'] = np.array([tweet.user.location for tweet in raw_tweets])
        df['Coordinates'] = np.array([tweet.coordinates for tweet in raw_tweets])
        df['Source'] = np.array([tweet.source for tweet in raw_tweets])
        df['Electoral_District'] = np.array([electorate_name for tweet in raw_tweets])
        df['Party'] = np.array([party_name for tweet in raw_tweets])
        df['State'] = np.array([state_name for tweet in raw_tweets])
        return df

    def politician_info_to_dataframe(self, user_info, state_name, electorate_name, party_name):
        """
        Convert politicians' raw data into structured data.
        :param user_info: User Status
            User raw data from twitter API.
        :param state_name: str
            State name of a politician.
        :param electorate_name: str
            Electoral District name of a politician.
        :param party_name: str
            Party name of a politician.

        :return: dataframe
            Structured dataframe.
        """
        df = pd.DataFrame()
        df['ID'] = [user_info.id_str]
        df['Name'] = [user_info.name]
        df['Screen_Name'] = [user_info.screen_name]
        df['Avatar'] = [user_info.profile_image_url.replace('_normal' or '_bigger', '')]
        df['Electoral_District'] = [electorate_name]
        df['Party'] = [party_name]
        df['Friends_Count'] = [user_info.friends_count]
        df['Followers_Count'] = [user_info.followers_count]
        df['Listed_Count'] = [user_info.listed_count]
        df['Total_Tweets'] = [user_info.statuses_count]
        df['Create_Time'] = [user_info.created_at]
        df['Location'] = [user_info.location]
        df['State'] = [state_name]
        df['Description'] = [user_info.description]
        return df

# db_address="115.146.85.107/"
    def save_data(self, tweets_list, db_name, collection_name, operation_type, db_address="103.6.254.48/"):
        """
        The function is used to update/insert data into MongoDB.
        :param tweets_list: list
            A list contains all preprocessed raw tweets.
        :param db_name: str
            Specify the DB we want to store in.
        :param collection_name: str
            Specify the collection we want to store in.
        :param operation_type: str
            Specify the type of operation.
        :param db_address: str
            Specify the db address to save data
        :return: null
        """
        try:
            client = MongoClient("mongodb://admin:123@" + db_address)
            # 203.101.225.125/")  # carol
            # client = MongoClient("mongodb://admin:123@115.146.85.107/")  # backend
            # client = MongoClient("mongodb://admin:123@103.6.254.48/")  # DB
            db = client[db_name]
            collection = db[collection_name]
            if operation_type == 'insert_one':
                collection.insert_one(tweets_list)
            elif operation_type == 'insert_many':
                collection.insert_many(tweets_list, ordered=False)
            elif operation_type == 'update':
                operations = []
                for item in tweets_list:
                    operations.append(UpdateOne({'ID': item['ID']}, {'$set': item}, upsert=True))
                collection.bulk_write(operations, ordered=False)
        except Exception as e:
            print(e)

    def find_data(self, db_name, collection_name, db_address="103.6.254.48/"):
        """
        The function is used to show all the data stored in MongoDB.
        :param db_name: str
            Specify the DB we want to obtain data.
        :param collection_name: str
            Specify the collection we want to obtain data.
        :param db_address: str
            Specify the db address to find data
        :return: list
            A list of results.
        """
        # client = MongoClient('mongodb+srv://chen:123@nlptest-r26bl.gcp.mongodb.net/test?retryWrites=true')
        # client = MongoClient("mongodb://admin:123@" + db_address)  # carol
        client = MongoClient("mongodb://admin:123@" + db_address)
        # 203.101.225.125/")  # carol
        # client = MongoClient("mongodb://admin:123@115.146.85.107/")  # backend
        # client = MongoClient("mongodb://admin:123@103.6.254.48/")  # DB
        db = client[db_name]
        collection = db[collection_name]
        return collection.find()

    def find_mongo_by_date(self, db_name, collection_name, start, end, db_address="103.6.254.48/"):
        """
        The function is used to show all the data stored in MongoDB.
        :param db_name: str
            This string is used to set specific db name.
        :param collection_name: string
            This string is used to set specific collection name.
        :param start: str
            Specify start date.
        :param end: str
            Specify end date.
        :param db_address: str
            Specify the db address to find data
        :return: list
            A list of results.
        """
        # client = MongoClient('mongodb+srv://chen:123@nlptest-r26bl.gcp.mongodb.net/test?retryWrites=true')

        client = MongoClient("mongodb://admin:123@" + db_address)  # carol
        # client = MongoClient("mongodb://admin:123@203.101.225.125/")  # carol
        # client = MongoClient("mongodb://admin:123@115.146.85.107/")
        db = client[db_name]
        collection = db[collection_name]
        # return collection.find({'Date': {'$lt': end}})
        return collection.find({'Date': {'$lt': end, '$gte': start}})

    def drop_collection(self, db_name, collection_name):
        client = MongoClient("mongodb://admin:123@115.146.85.107/")  # backend
        # client = MongoClient("mongodb://admin:123@103.6.254.48/") #DB
        db = client[db_name]
        collection = db[collection_name]
        collection.drop()

    def count_popular_hashtag(self, tweets_df):
        """
        Calculate top six hashtags.
        :param tweets_df:
        :return:
        """
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
        top_tags = tag_count.most_common(5)
        return top_tags
