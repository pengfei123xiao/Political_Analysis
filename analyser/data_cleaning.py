#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 10/05/2019 10:00 AM
# @Author  : Zhihui Cheng
# @FileName: data_cleaning.py
# @Software: PyCharm

import functional_tools
import sys
import pandas as pd
import time


# move data within different instances
def move_data(db_name, collection_name):
    try:
        f_tools = functional_tools.FunctionalTools()
        loc_from_mongo = f_tools.find_data(db_name, "locToCooOneDay", "203.101.225.125/")
        loc_df = pd.DataFrame(list(loc_from_mongo))
        del loc_from_mongo
        print(loc_df.shape)
        loc_target_from_mongo = f_tools.find_data(db_name, collection_name)
        loc_target_df = pd.DataFrame(list(loc_target_from_mongo))
        print(loc_target_df.shape)
        new_loc_df = loc_df[~ loc_df['Location'].isin(loc_target_df['Location'])]
        del loc_target_from_mongo
        print(new_loc_df.shape)
        f_tools.save_data(new_loc_df.to_dict('records'), 'backup', collection_name, 'insert_many')
    except Exception as e:
        print(e)


# save data to database
def save_data(db_name, collection_name):
    try:
        f_tools = functional_tools.FunctionalTools()
        read_start_time = time.time()
        data_from_mongo = f_tools.find_data(db_name, collection_name)
        data_list = list(data_from_mongo)[:100000]
        print(time.time() - read_start_time)
        del data_from_mongo
        save_start_time = time.time()
        f_tools.save_data(data_list, db_name, collection_name, "update")
        print(time.time() - save_start_time)
    except Exception as e:
        print(e)


# drop duplicate for collections
def drop_duplicate(db_name, collection_name):
    try:
        f_tools = functional_tools.FunctionalTools()
        loc_from_mongo = f_tools.find_data(db_name, collection_name)
        loc_df = pd.DataFrame(list(loc_from_mongo))
        del loc_from_mongo
        print(loc_df.shape)
        loc_df.drop_duplicates(subset="Location",
                               keep="first", inplace=True)
        # print(loc_df.shape)
        f_tools.save_data(loc_df.to_dict('records'), 'backup', "locToCooBAKK", 'insert_many')
    except Exception as e:
        print(e)


if __name__ == '__main__':
    db_name = sys.argv[1]
    collection_name = sys.argv[2]
    print(db_name)
    print(collection_name)
    save_data(db_name, collection_name)
    # move_data(db_name,collection_name)
    # drop_duplicate(db_name,collection_name)
