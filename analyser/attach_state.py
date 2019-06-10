#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 12/05/2019 10:12 PM
# @Author  : Pengfei Xiao, Zhihui Cheng
# @FileName: attach_coordinates.py
# @Software: PyCharm
import sys
import threading
import time
from datetime import datetime
import logging
import fiona
import googlemaps
import pandas as pd
from shapely.geometry import Point, shape

sys.path.append('..')
from analyser import functional_tools
import gc

gc.enable()

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler('attach_state.log')
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


class AttachState(threading.Thread):
    def __init__(self, start_date, end_date, f_tools, totalMention_df, gmap_key):
        """
        :param :
        """

        threading.Thread.__init__(self)
        self.gmap_key = gmap_key
        self.totalMention_df = totalMention_df
        self.f_tools = f_tools
        self.totalMention_df.drop_duplicates(subset ='ID',keep = "first", inplace = True)


    def calculate_new_locations(self):
        # new_mention_df = self.new_mention_df[self.new_mention_df['Location'] != '']
        loc_result_from_mongo = self.f_tools.find_data('backup', 'locToCoo')
        loc_result_df = pd.DataFrame(list(loc_result_from_mongo))
        del loc_result_from_mongo
        gc.collect()
        new_loc_list = list(set(self.totalMention_df['Location'].unique()) - set(loc_result_df['Location']))
        logger.info("Total {} new locations to calcuate coordinate".format(len(new_loc_list)))
        del loc_result_df
        gc.collect()
        return new_loc_list

    # convert the location to coordinate by google api
    def location_to_coordinate(self, location):
        try:
            logger.info(location)
            geocode_result = self.gmap_key.geocode(location)
            time.sleep(1)
            # logger.info(geocode_result)
            lat = geocode_result[0]['geometry']['location']['lat']
            lng = geocode_result[0]['geometry']['location']['lng']
        except Exception as e:
            logger.error("location to coordinate error ", e)
            return None
        return (lat, lng)

    # save the coordinate and location pair to database
    def calculate_coordinates(self, new_loc_list):
        logger.info('Thread to calculate coordinates from location starts.')
        convert_df = pd.DataFrame()
        convert_df['Location'] = new_loc_list
        convert_df['Coordinates'] = [self.location_to_coordinate(item) for item in new_loc_list]
        functional_tools.FunctionalTools().save_data(convert_df.to_dict('records'), 'backup', 'locToCoo',
                                                     'insert_many')
        logger.info('Thread to calculate coordinates from location ends.')
        del convert_df
        gc.collect()

    # calculate the state by check the coordinate in the polygon
    def calculate_state_by_coordinate(self, coordinate, polygons):
        # [96.8168,-43.7405,159.1092,-9.1422] boudning box of australia
        if 96.8168 <= coordinate[1] <= 159.1092 and -43.7405 <= coordinate[0] <= -9.1422:
            for poly in polygons:
                if shape(Point(coordinate[1], coordinate[0])).within(shape(poly['geometry'])):
                    # print("The state is {}".format(poly['properties']['STE_NAME16']))
                    return poly['properties']['STE_NAME16']
        return None

    # calculate and attach the state to mentioned collection
    def calculate_state(self):
        logger.info('Thread to calculate states starts.')
        loc_result_from_mongo = self.f_tools.find_data('backup', 'locToCoo')
        loc_result_df = pd.DataFrame(list(loc_result_from_mongo))
        del loc_result_from_mongo
        gc.collect()
        logger.info('Thread to read locations ends.')
        logger.info(loc_result_df.shape)
        logger.info(len(list(loc_result_df['Location'].unique())))
        loc_result_df = loc_result_df.dropna()
        loc_result_df.rename(columns={"Coordinates": "NewCoordinates"}, inplace=True)
        logger.info("loc_result_df: {}".format(loc_result_df.shape))
        # merge mention_tweets with new coordinates
        new_mention_df = pd.merge(self.totalMention_df,
                                  loc_result_df[['Location', 'NewCoordinates']],
                                  on='Location',
                                  how='left')
        new_mention_df = new_mention_df.dropna(subset=['NewCoordinates'])
        logger.info("mention_df: {}".format(self.totalMention_df.shape))
        logger.info("new_mention_df: {}".format(new_mention_df.shape))

        polygons = fiona.open("../data/STE_2016_AUST/STE_2016_AUST.shp")

        time0 = datetime.now()
        new_mention_df['State'] = new_mention_df['NewCoordinates'].apply(
            lambda x: self.calculate_state_by_coordinate(x, polygons))
        time1_diff = datetime.now() - time0
        logger.info("time of calculating state: {}".format(time1_diff))

        # attach states
        final_mention_df = pd.merge(self.totalMention_df,
                                    new_mention_df[['ID', 'State']],
                                    on='ID',
                                    how='left')

        del new_mention_df
        gc.collect()
        final_mention_df = final_mention_df.where((pd.notnull(final_mention_df)), None)
        logger.info('final_mention_df: {}'.format(final_mention_df.shape))
        logger.info('length of final_mention_df: {}'.format(len(list(final_mention_df['ID'].unique()))))

        time1 = datetime.now()
        self.f_tools.save_data(final_mention_df.to_dict('records'), 'backup', 'totalMentionedWithState', 'insert_many')
        logger.info("time of save data with state:{}".format(datetime.now() - time1))
        del final_mention_df
        gc.collect()

    # run the thread
    def run(self):
        new_loc_list = self.calculate_new_locations()
        self.calculate_coordinates(new_loc_list)
        self.calculate_state()


if __name__ == '__main__':
    # discarded, call from analysis manager
    start_date = datetime(2019, 5, 12)
    end_date = datetime(2019, 5, 18)
    start_time = time.time()
    f_tools = functional_tools.FunctionalTools()

    totalMention_from_mongo = f_tools.find_mongo_by_date('capstone', 'streamingMentionedCorrectDate', start_date,
                                                         end_date)

    totalMention_df = pd.DataFrame(list(totalMention_from_mongo))
    del totalMention_from_mongo
    gc.collect()

    print('Data loaded. Time used: %f mins' % ((time.time() - start_time) / 60))

    gmap_key3 = googlemaps.Client(key='AIzaSyBEGnKK5sbPBXi2tL4o7LFahhEniTaLQTY', timeout=200)  # edward's key
    temp_df = totalMention_df[totalMention_df['Location'] != ''].copy()
    loc_result_from_mongo = f_tools.find_data('backup', 'locToCoo')
    loc_result_df = pd.DataFrame(list(loc_result_from_mongo))
    new_loc_list = list(set(temp_df['Location'].unique()) - set(loc_result_df['Location']))
    print(len(new_loc_list))
    print(len(set(new_loc_list)))
    del loc_result_from_mongo, loc_result_df, totalMention_df, temp_df
    gc.collect()
    batch_size = int(len(new_loc_list) / 3)

    print('Start converting. Time used: %f mins' % ((time.time() - start_time) / 60))
    thread3 = AttachCoordinate(new_loc_list, gmap_key3, 3)
    thread3.start()
    thread3.join()
    print('Finished. Time used: %f mins' % ((time.time() - start_time) / 60))