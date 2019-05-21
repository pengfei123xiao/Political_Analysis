#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 12/05/2019 10:12 PM
# @Author  : Pengfei Xiao, Zhihui Cheng
# @FileName: attach_coordinates.py
# @Software: PyCharm
import fiona
from shapely.geometry import MultiPoint, Point, Polygon,shape
from shapely.geometry.polygon import Polygon
import pandas as pd
import numpy as np
import googlemaps
from multiprocessing import Process
import threading
import time
import sys
from datetime import datetime

sys.path.append('..')
from analyser import functional_tools
import gc

gc.enable()


# class AttachCoordinate(Process):
class AttachState(threading.Thread):
    def __init__(self, start_date, end_date, f_tools, totalMention_df, gmap_key):
        """
        :param twitter_user:
        """
        # super().__init__()
        threading.Thread.__init__(self)
        self.gmap_key = gmap_key
        self.totalMention_df = totalMention_df
        self.f_tools = f_tools
        self.new_mention_df = self.totalMention_df[(self.totalMention_df['Date'] >= start_date) & (self.totalMention_df['Date'] < end_date)]

    def calculate_new_locations(self):
        new_mention_df = self.new_mention_df[self.new_mention_df['Location'] != '']
        loc_result_from_mongo = self.f_tools.find_data('backup','locToCoo', "115.146.85.107/")
        loc_result_df=pd.DataFrame(list(loc_result_from_mongo))
        del loc_result_from_mongo
        new_loc_list = list(set(new_mention_df['Location'].unique())-set(loc_result_df['Location']))
        print("Total {} new locations to calcuate coordinate".format(len(new_loc_list)))
        return new_loc_list

    def location_to_coordinate(self, location):
        try:
            # print(location)
            geocode_result = self.gmap_key.geocode(location)
            lat = geocode_result[0]['geometry']['location']['lat']
            lng = geocode_result[0]['geometry']['location']['lng']
        except Exception as e:
            print("location to coordinate error ",e)
            return None
        return (lat, lng)

    def calculate_coordinates(self, new_loc_list):
        print('Thread to calculate coordinates from location starts.')
        convert_df = pd.DataFrame()
        convert_df['Location'] = new_loc_list
        convert_df['Coordinates'] = [self.location_to_coordinate(item) for item in new_loc_list]
        functional_tools.FunctionalTools().save_data(convert_df.to_dict('records'), 'backup', 'locToCoo',
                                                     'insert_many')
        print('Thread to calculate coordinates from location ends.')

    def calculate_state_by_coordinate(self, coordinate, polygons):
        # [96.8168,-43.7405,159.1092,-9.1422] boudning box of australia
      if 96.8168<= coordinate[1]<=159.1092 and -43.7405<=coordinate[0]<=-9.1422:
          for poly in polygons:
                  if shape(Point(coordinate[1],coordinate[0])).within(shape(poly['geometry'])):
                      # print("The state is {}".format(poly['properties']['STE_NAME16']))
                      return poly['properties']['STE_NAME16']
      return None

    def calculate_state(self):
        loc_result_from_mongo = self.f_tools.find_data('backup','locToCoo', "115.146.85.107/")
        loc_result_df=pd.DataFrame(list(loc_result_from_mongo))
        del loc_result_from_mongo
        loc_result_df = loc_result_df.dropna()
        loc_result_df.rename(columns={"Coordinates": "NewCoordinates"}, inplace=True)
        print("loc_result_df: ",loc_result_df.shape)

        # merge mention_tweets with new coordinates
        new_mention_df = pd.merge(self.new_mention_df,
                         loc_result_df[['Location', 'NewCoordinates']],
                         on='Location',
                         how='left')
        new_mention_df = new_mention_df.dropna(subset = ['NewCoordinates'])

        print('new_mention_df:', new_mention_df.shape)

        polygons = fiona.open("../data/STE_2016_AUST/STE_2016_AUST.shp")

        time0 = datetime.now()
        new_mention_df['State'] = new_mention_df['NewCoordinates'].apply(lambda x: self.calculate_state_by_coordinate(x,polygons))
        time1_diff = datetime.now() - time0
        print("time of calculating state: {}".format(time1_diff))

        # attach states
        final_mention_df = pd.merge(self.new_mention_df,
                         new_mention_df[['ID', 'State']],
                         on='ID',
                         how='left')

        del new_mention_df
        final_mention_df = final_mention_df.where((pd.notnull(final_mention_df)), None)
        print('final_mention_df: {}'.format(final_mention_df.shape))

        time1 = datetime.now()
        self.f_tools.save_data(final_mention_df.to_dict('records'), 'backup', 'totalMentionedWithState','insert_many')
        print("time of save data with state:{}".format(datetime.now() - time1))
        del final_mention_df

    def run(self):
        new_loc_list = self.calculate_new_locations()
        self.calculate_coordinates(new_loc_list)
        self.calculate_state()

if __name__ == '__main__':
    # start_date = datetime.datetime.today() - datetime.timedelta(days=1)
    start_date = datetime(2019, 5, 12)
    end_date = datetime(2019, 5, 18)
    start_time = time.time()
    f_tools = functional_tools.FunctionalTools()

    # totalMention_from_mongo = f_tools.find_data('backup', 'totalMentioned')
    totalMention_from_mongo = f_tools.find_mongo_by_date('capstone', 'streamingMentionedCorrectDate', start_date, end_date)

    totalMention_df = pd.DataFrame(list(totalMention_from_mongo))
    del totalMention_from_mongo

    print('Data loaded. Time used: %f mins' % ((time.time() - start_time) / 60))

    # gmap_key1 = googlemaps.Client(key='AIzaSyAnnzRxDBvuUIQgj7AzbVKmYfoNNHCuG7U', timeout=200)

    # gmap_key2 = googlemaps.Client(key='AIzaSyDYUzZHXqyPCijvhO6IL9Eo4RyjnL0M9yA', timeout=200)
    gmap_key3 = googlemaps.Client(key='AIzaSyBEGnKK5sbPBXi2tL4o7LFahhEniTaLQTY', timeout=200)  # edward
    temp_df = totalMention_df[totalMention_df['Location'] != ''].copy()
    # daily_df['Coordinates']=daily_df['Location'].apply(lambda x: location_to_coordinate(x))
    # loc_arr = np.unique(temp_df['Location'].astype(str).values)
    # print(type(loc_arr))
    # print('Unique location: {}'.format(loc_arr.size))


    # "115.146.85.107/",
    loc_result_from_mongo = f_tools.find_data('backup','locToCoo')
    loc_result_df=pd.DataFrame(list(loc_result_from_mongo))
    new_loc_list = list(set(temp_df['Location'].unique())-set(loc_result_df['Location']))
    print(len(new_loc_list))
    print(len(set(new_loc_list)))
    del loc_result_from_mongo, loc_result_df, totalMention_df, temp_df
    gc.collect()
    # batch_size = int(loc_arr.size / 3)
    batch_size = int(len(new_loc_list) / 3)

    print('Start converting. Time used: %f mins' % ((time.time() - start_time) / 60))
    # thread1 = AttachCoordinate(new_loc_list[:batch_size], gmap_key1, 1)
    # thread2 = AttachCoordinate(new_loc_list[batch_size:batch_size * 2], gmap_key2, 2)
    # thread3 = AttachCoordinate(new_loc_list[batch_size * 2:], gmap_key3, 3)
    thread3 = AttachCoordinate(new_loc_list, gmap_key3, 3)

    # thread1.start()
    # thread2.start()
    thread3.start()
    # thread1.join()
    # thread2.join()
    thread3.join()
    print('Finished. Time used: %f mins' % ((time.time() - start_time) / 60))
