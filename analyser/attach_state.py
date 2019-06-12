#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 12/05/2019 10:12 PM
# @Author  : Pengfei Xiao, Zhihui Cheng
# @FileName: attach_state.py
# @Software: PyCharm
import sys

sys.path.append('..')
import threading
import time
from datetime import datetime
import logging
import fiona
import googlemaps
import pandas as pd
from shapely.geometry import Point, shape
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
    def __init__(self, f_tools, totalMention_df, gmap_key):
        threading.Thread.__init__(self)
        self.gmap_key = gmap_key
        self.totalMention_df = totalMention_df
        self.f_tools = f_tools
        self.totalMention_df.drop_duplicates(subset='ID', keep="first", inplace=True)

    def calculate_new_locations(self):
        """
        Extract new location that are different from the location in the 'locToCoo' collection.

        :return: list
            A list new containing new location.
        """
        loc_result_from_mongo = self.f_tools.find_data('backup', 'locToCoo')
        loc_result_df = pd.DataFrame(list(loc_result_from_mongo))
        del loc_result_from_mongo
        gc.collect()
        new_loc_list = list(set(self.totalMention_df['Location'].unique()) - set(loc_result_df['Location']))
        logger.info("Total {} new locations to calcuate coordinate".format(len(new_loc_list)))
        del loc_result_df
        gc.collect()
        return new_loc_list

    def location_to_coordinate(self, location):
        """
        Convert the location to coordinate by google api.
        :param location: str
            Location description from twitter.

        :return: tuple
            Coordinate tuple.
        """
        try:
            logger.info(location)
            geocode_result = self.gmap_key.geocode(location)
            time.sleep(1)
            lat = geocode_result[0]['geometry']['location']['lat']
            lng = geocode_result[0]['geometry']['location']['lng']
        except Exception as e:
            logger.error("location to coordinate error ", e)
            return None
        return (lat, lng)

    def calculate_coordinates(self, new_loc_list):
        """
        Save the coordinate and location pair to database.
        :param new_loc_list: list
            A list contains all new locations.

        :return: None
        """
        logger.info('Thread to calculate coordinates from location starts.')
        convert_df = pd.DataFrame()
        convert_df['Location'] = new_loc_list
        convert_df['Coordinates'] = [self.location_to_coordinate(item) for item in new_loc_list]
        functional_tools.FunctionalTools().save_data(convert_df.to_dict('records'), 'backup', 'locToCoo',
                                                     'insert_many')
        logger.info('Thread to calculate coordinates from location ends.')
        del convert_df
        gc.collect()

    def calculate_state_by_coordinate(self, coordinate, polygons):
        """
        Calculate the state by check the coordinate in the polygon.
        :param coordinate: list
            Coordinate.
        :param polygons: list
            Polygon information.

        :return: State information.
        """
        # [96.8168,-43.7405,159.1092,-9.1422] bounding box of australia
        if 96.8168 <= coordinate[1] <= 159.1092 and -43.7405 <= coordinate[0] <= -9.1422:
            for poly in polygons:
                if shape(Point(coordinate[1], coordinate[0])).within(shape(poly['geometry'])):
                    return poly['properties']['STE_NAME16']
        return None

    def calculate_state(self):
        """
        Calculate and attach the state to mentioned collection.

        :return: None
        """
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
