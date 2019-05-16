#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 12/05/2019 10:12 PM
# @Author  : Pengfei Xiao
# @FileName: attach_coordinates.py
# @Software: PyCharm

import pandas as pd
import numpy as np
import googlemaps
from multiprocessing import Process
import threading
import time
import sys

sys.path.append('..')
from analyser import functional_tools
import gc

gc.enable()


# class AttachCoordinate(Process):
class AttachCoordinate(threading.Thread):
    def __init__(self, location_list, gmap_key, index):
        """
        :param twitter_user:
        """
        # super().__init__()
        threading.Thread.__init__(self)
        self.location_list = location_list
        self.gmap_key = gmap_key
        self.index = index

    def location_to_coordinate(self, location):
        geocode_result = self.gmap_key.geocode(location)
        try:
            lat = geocode_result[0]['geometry']['location']['lat']
            lng = geocode_result[0]['geometry']['location']['lng']
        except:
            return None
        return (lat, lng)

    def run(self, ):
        print('Thread {} starts.'.format(str(self.index)))
        convert_df = pd.DataFrame()
        convert_df['Location'] = self.location_list
        convert_df['Coordinates'] = [self.location_to_coordinate(item) for item in self.location_list]
        functional_tools.FunctionalTools().save_data(convert_df.to_dict('records'), 'backup', 'locToCoo',
                                                     'insert_many')
        print('Thread {} ends.'.format(str(self.index)))
        # for item in self.loc_list:
        #     self.location_to_coordinate(item)


if __name__ == '__main__':
    start_time = time.time()
    f_tools = functional_tools.FunctionalTools()
    totalMention_from_mongo = f_tools.find_data('backup', 'totalMentioned')
    totalMention_df = pd.DataFrame(list(totalMention_from_mongo))
    del totalMention_from_mongo
    gc.collect()
    print('Data loaded. Time used: %f mins' % ((time.time() - start_time) / 60))
    gmap_key1 = googlemaps.Client(key='AIzaSyAnnzRxDBvuUIQgj7AzbVKmYfoNNHCuG7U', timeout=200)
    gmap_key2 = googlemaps.Client(key='AIzaSyDYUzZHXqyPCijvhO6IL9Eo4RyjnL0M9yA', timeout=200)
    gmap_key3 = googlemaps.Client(key='AIzaSyBEGnKK5sbPBXi2tL4o7LFahhEniTaLQTY', timeout=200)  # edward
    temp_df = totalMention_df[totalMention_df['Location'] != ''].copy()
    # daily_df['Coordinates']=daily_df['Location'].apply(lambda x: location_to_coordinate(x))
    # loc_arr = np.unique(temp_df['Location'].astype(str).values)
    loc_arr = np.unique(temp_df['Location'].astype(str).values)
    print('Unique location: {}'.format(loc_arr.size))
    del totalMention_df, temp_df
    gc.collect()
    batch_size = int(loc_arr.size / 3)
    print('Start converting. Time used: %f mins' % ((time.time() - start_time) / 60))
    thread1 = AttachCoordinate(loc_arr[:batch_size], gmap_key1, 1)
    thread2 = AttachCoordinate(loc_arr[batch_size:batch_size * 2], gmap_key2, 2)
    thread3 = AttachCoordinate(loc_arr[batch_size * 2:], gmap_key3, 3)
    thread1.start()
    thread2.start()
    thread3.start()
    thread1.join()
    thread2.join()
    thread3.join()
    print('Finished. Time used: %f mins' % ((time.time() - start_time) / 60))
