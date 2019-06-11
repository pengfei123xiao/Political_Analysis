#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 27/03/2019 12:28 AM
# @Author  : Pengfei Xiao
# @FileName: harvester_manager.py
# @Software: PyCharm

"""This file is used to manage restful harvester that crawling mention and reply to the politicians."""

import pandas as pd
import time
import sys

sys.path.append('..')
from restful_replies import RestfulReplies
from restful_by_mentioned import RestfulByMentioned
import gc

gc.enable()

if __name__ == '__main__':
    start_time = time.time()

    temp_df = pd.read_csv('../data/full_politician_list.csv', usecols=['Screen_Name'])
    politician_list = temp_df['Screen_Name'].dropna().tolist()

    print("Start restful crawling.")
    for screen_name in politician_list:
        print('============================================')
        print('Process: {}/{}'.format(politician_list.index(screen_name) + 1, len(politician_list)))
        restful_mentioned = RestfulByMentioned(screen_name, 'capstone', 'streamingMentionedCorrectDate')
        restful_replies = RestfulReplies(screen_name, 'capstone', 'streamingMentionedCorrectDate')
        print("Crawling tweets mentioned {}.".format(screen_name))
        restful_mentioned.start()
        print("Crawling replies to {}.".format(screen_name))
        restful_replies.start()
        restful_replies.join()
        restful_mentioned.join()

    print('Finished. Time used: %f mins' % ((time.time() - start_time) / 60))

    gc.collect()
