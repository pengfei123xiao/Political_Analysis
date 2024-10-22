#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 10/04/2019 10:05 AM
# @Author  : Zhihui Cheng
# @FileName: streaming_by_mentioned.py
# @Software: PyCharm

import sys
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler, AppAuthHandler, Stream, API

sys.path.append('..')
from analyser import functional_tools
import pandas as pd
import time
from pymongo import MongoClient
import logging
from config import config
import gc

gc.enable()

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler('stream-api.log')
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


class MyStreamListener(StreamListener):
    """
    This is a listener that deals with received tweets.
    """

    def __init__(self, db_address, database, collection, tweetAnalyser, screen_name_list):
        self.database = database
        self.collection = collection
        self.analyser = tweetAnalyser
        self.screen_name_list = screen_name_list
        self.tweets = []
        self.tweet_count = 0
        self.db_address = db_address

    def on_data(self, data):
        try:
            self.tweet_count += 1
            self.tweets.append(data)
            if self.tweet_count % 100 == 0:
                start_time = time.time()
                try:
                    # process data harvested
                    self.analyser.save_data(self.analyser.filter_tweet_streaming(self.tweets), self.database,
                                            self.collection, 'insert_many')
                    logger.info("100 tweets was saved")
                    logger.info("spent time: %s", (time.time() - start_time) / 60)
                except Exception as e:
                    logger.error(e)
                self.tweet_count = 0
                self.tweets = []

            return True
        except BaseException as e:
            logger.error("Error on_data %s" % str(e))
        return True

    def on_status(self, status):
        logger.info("On status", status)
        if status.retweeted_status:
            return

    # Handle errors
    def on_error(self, status):
        if status == 401:
            logger.error("Error on_error 401: Missing or incorrect authentication credentials.")
            return False

        if status == 304:
            logger.error("Error on_error 304: There was no new data to return.")
            return False

        if status == 400:
            logger.error("Error on_error 400: The request was invalid or cannot be otherwise served.")
            return False

        if status == 502:
            logger.error("Error on_error 502: Twitter is down, or being upgraded.")
            return False

        if status == 503:
            logger.error(
                "Error on_error 503: The Twitter servers are up, but overloaded with requests. Try again later.")
            return False

        if status == 504:
            logger.error(
                "Error on_error 504: The Twitter servers are up, but the request couldn’t be serviced due to some failure within the internal stack.")
            return False

        if status == 420:
            logger.error("Error on_error 420: Rate limited for making too many requests.")
            time.sleep(60)
            return False


class TwitterStreamer():
    def __init__(self, auth):
        self.auth = auth

    def stream_tweets(self, db_address, database, collection, search_list):
        while True:
            try:
                # Connect to Twitter Streaming API
                listener = MyStreamListener(db_address, database, collection, functional_tools.FunctionalTools(),
                                            search_list)
                stream = Stream(self.auth, listener)

                # Filter Twitter Streams to capture data by the keywords:
                stream.filter(track=search_list)
            except Exception as e:
                logger.error("Error in stream_tweets: ", e)
                continue


if __name__ == '__main__':
    # Authenticate using config.py and connect to Twitter Streaming API.
    logger.info("Start crawling.")
    # Use processor 7/8
    harvester_id = int(sys.argv[1])
    conf = config[harvester_id]
    auth = functional_tools.FunctionalTools().authenticate_twitter_app(conf['consumer_key'], conf['consumer_secret'])

    # Run Streaming to get real time tweets
    twitter_streamer = TwitterStreamer(auth)

    # Get Politician list
    temp_df = pd.read_csv('../data/full_politician_list.csv', usecols=['ScreenName'])
    temp_df = temp_df[temp_df['ScreenName'] != 'NF']['ScreenName'].dropna()
    temp_df = temp_df.apply(lambda x: '@' + x)
    politician_screen_name_list = temp_df.tolist()
    print(politician_screen_name_list)
    db_address = conf['mongodb_address']
    database = conf['mongodb_db_name']
    collection = conf['mongodb_collection_name']
    # Start harvester
    twitter_streamer.stream_tweets(db_address, database, collection, politician_screen_name_list)

    gc.collect()
