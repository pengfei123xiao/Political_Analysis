#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 12/05/2019 10:05 AM
# @Author  : Zhihui Cheng
# @FileName: analysis_manager.py
# @Software: PyCharm

import sys

sys.path.append('..')
from restful_harvester import restful_pol_tweets, restful_pol_info, restful_by_hashtag
import functional_tools, attach_state, cumulative_analysis, daily_analysis
import pandas as pd
from datetime import datetime, timedelta, date, time as dtime
import time
import googlemaps
import logging
import gc

gc.enable()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler('analysis_manager.log')
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

if __name__ == '__main__':
    try:
        # set parameters
        start_date = datetime(2019, 4, 13, 14, 0, 0)
        # end_date = datetime(2019, 4, 26)
        # start_date = datetime.combine(date.today() - timedelta(days=2), dtime(14, 0))
        end_date = datetime(2019, 4, 15, 14, 0, 0)
        # end_date = datetime.combine(date.today()- timedelta(days=30), dtime(14, 0))
        # end_date = datetime.combine(date.today()- timedelta(days=1), dtime(14, 0))
        logger.info('============================================')
        logger.info("Start date: {}; end date: {}.".format(start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')))
        find_ip_address = "115.146.85.107/"
        save_ip_address = ""
        db_name = "backup"
        politician_tweet_collection_name = "restfulTweets"
        politician_info_collection_name = "politicianInfo"
        hashtag_collection_name = "restfulByHashtag"
        f_tools = functional_tools.FunctionalTools()

        # read politician info
        temp_df = pd.read_csv('../data/full_politician_list.csv',
                              usecols=['Name', 'State', 'Electorate', 'Party', 'Screen_Name'])
        politician_list = temp_df['Screen_Name'].dropna().tolist()
        state_list = temp_df['State'].dropna().tolist()
        ele_list = temp_df['Electorate'].dropna().tolist()
        party_list = temp_df['Party'].dropna().tolist()

        # update info for politicians
        result_dict = {}
        logger.info('============================================')
        logger.info("Harvest {} politician's info starts.".format(len(politician_list)))
        pol_info_start_time = time.time()
        for i in range(len(politician_list[:2])):
            pol_info_thread = restful_pol_info.RestfulUserInfo(politician_list[i], db_name,
                                                               politician_info_collection_name, state_list[i], ele_list[i],
                                                               party_list[i])
            # logger.info("Crawling information of {}.".format(politician_list[i]))
            pol_info_thread.start()

        # harvest politician's tweets
        logger.info('============================================')
        logger.info("Harvest {} politician's tweets starts.".format(len(politician_list)))
        pol_tweets_start_time = time.time()
        for i in range(len(politician_list[:2])):
            pol_tweets_thread = restful_pol_tweets.RestfulPolTweets(politician_list[i], db_name,
                                                                    politician_tweet_collection_name, state_list[i],
                                                                    ele_list[i], party_list[i], start_date)
            # logger.info("Crawling tweets of {}.".format(politician_list[i]))
            pol_tweets_thread.start()

        pol_info_thread.join()
        logger.info("Harvest politician's info finishes. {} mins used ".format((time.time() - pol_info_start_time) / 60))

        # # read politician data from mongoDB
        politician_from_mongo = f_tools.find_data(db_name, 'politicianInfo')
        politician_df = pd.DataFrame(list(politician_from_mongo))
        del politician_from_mongo
        gc.collect()
        # save politicianInfo to daily
        result_dict['Date'] = datetime.combine(date.today() - timedelta(days=1), datetime.min.time())
        result_dict['data'] = politician_df.to_dict('records')
        f_tools.save_data(result_dict, db_name, 'dailyPolInfo', 'insert_one')

        pol_tweets_thread.join()
        logger.info(
            "Harvest politician's tweets finishes. {} mins used.".format((time.time() - pol_tweets_start_time) / 60))

        # read politician's tweets
        read_pol_tweets_start_time = datetime.now()
        pol_tweets = f_tools.find_mongo_by_date(db_name, politician_tweet_collection_name, start_date, end_date)
        pol_tweets_df = pd.DataFrame(list(pol_tweets))
        del pol_tweets
        gc.collect()
        logger.info("Politician's tweets read time: {}".format(datetime.now() - read_pol_tweets_start_time))

        # # calculate top five hash tags of politicians' tweets
        # top_tags = f_tools.count_popular_hashtag(pol_tweets_df)
        # hashtag_list = ["#" + tag[0] for tag in top_tags]

        # # harvest user's tweets for top five hash tags
        # hashtag_tweets_start_time = time.time()
        # logger.info('============================================')
        # logger.info('Harvest tweets with hashtags starts')
        # restful_hashtag = restful_by_hashtag.RestfulHashtags(start_date, hashtag_list, db_name, hashtag_collection_name)
        # restful_hashtag.start()

        # read user's tweets who mentioned the politicians
        logger.info('============================================')
        logger.info("Read mentioned tweets starts")
        read_men_tweets_start_time = time.time()
        totalMention_from_mongo = f_tools.find_mongo_by_date('capstone', 'streamingMentionedCorrectDate',
                                                             start_date, end_date, find_ip_address)
        totalMention_df = pd.DataFrame(list(totalMention_from_mongo))
        logger.info('{} mentioned tweets loaded. Time used: {} mins'.format(len(totalMention_df.index), (
                time.time() - read_men_tweets_start_time) / 60))
        del totalMention_from_mongo
        gc.collect()

        gmap_key = googlemaps.Client(key='AIzaSyBEGnKK5sbPBXi2tL4o7LFahhEniTaLQTY', timeout=200)  # edward
        attach_state_thread = attach_state.AttachState(start_date, end_date, f_tools, totalMention_df, gmap_key)

        attach_state_thread.start()
        logger.info('============================================')
        logger.info('Attach state starts.')
        attach_state_thread.join()
        logger.info('Attach state finishes.')
        logger.info('============================================')
        logger.info("Cumulative analysis starts")
        cumulative_analysis_start_time = time.time()
        cumulative_analysis = cumulative_analysis.CumulativeAnalysis(start_date, end_date, f_tools,
                                                                     politician_df)
        cumulative_analysis.start()

        cumulative_analysis.join()
        logger.info(
            'Cumulative analysis finishes. Time used: %f mins' % ((time.time() - cumulative_analysis_start_time) / 60))

        # restful_hashtag.join()
        # logger.info(
        #     'Harvest tweets with hashtags finished. Time used: %f mins' % ((time.time() - hashtag_tweets_start_time) / 60))
        logger.info('============================================')
        logger.info("Daily analysis starts")
        daily_analysis_start_time = time.time()
        daily_analysis = daily_analysis.DailyAnalysis(start_date, end_date, f_tools,
                                                      totalMention_df, pol_tweets_df, politician_df)

        daily_analysis.start()
        daily_analysis.join()
        logger.info('Daily analysis finishes. Time used: %f mins' % ((time.time() - daily_analysis_start_time) / 60))
        gc.collect()
    except Exception as e:
        logger.error(e)
