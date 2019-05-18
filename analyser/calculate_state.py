# File: calculate_state.py
# Author: Zhihui Cheng
# Purpose: Tweets pre-processing, get the state for each tweet by coordinate

from shapely.geometry import Point
from shapely.geometry.polygon import Polygon
import json

# Calculate the state geocode for each tweet
def calculate_state(geo, polygons):
    polygon_result = {}
    point = Point(geo[0], geo[1])
    for polygon in polygons:
        polygon_points = Polygon(polygon[0])
        if polygon_points.contains(point):
            polygon_result['State_Code'] = polygon[1]['STE_CODE16']
            polygon_result['State_Name'] = polygon[1]['STE_NAME16']
            return polygon_result
    return None

# extract polygons information from polygon json file
def get_polygon(polygon_file):
    # load polygon information from file
    polygons = []
    with open('Data/' + polygon_file, 'r') as f:
        for state in json.loads(f.read())['features']:
            coordinates = []
            if state['geometry']['type'] == 'Polygon':
                for coordinate in state['geometry']['coordinates'][0]:
                    log, lat = coordinate
                    coordinates.append((log, lat))
            elif state['geometry']['type'] == 'MultiPolygon':
                for values in state['geometry']['coordinates']:
                    for coordinate in values[0]:
                        log, lat = coordinate
                        coordinates.append((log, lat))
            polygons.append((coordinates, state['properties']))
    return polygons

print(calculate_state([144.9851253,-37.887716],(get_polygon("STE_2016_AUST.json"))))
