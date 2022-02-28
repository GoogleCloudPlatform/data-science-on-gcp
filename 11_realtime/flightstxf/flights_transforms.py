#!/usr/bin/env python3

# Copyright 2021 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import apache_beam as beam
import datetime as dt
import logging
import numpy as np
import farmhash  # pip install pyfarmhash

DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
WINDOW_DURATION = 60 * 60
WINDOW_EVERY = 5 * 60


def get_data_split(fl_date):
    fl_date_str = str(fl_date)
    # Use farm fingerprint just like in BigQuery
    x = np.abs(np.uint64(farmhash.fingerprint64(fl_date_str)).astype('int64') % 100)
    if x < 60:
        data_split = 'TRAIN'
    elif x < 80:
        data_split = 'VALIDATE'
    else:
        data_split = 'TEST'
    return data_split


def get_data_split_2019(fl_date):
    fl_date_str = str(fl_date)
    if fl_date_str > '2019':
        data_split = 'TEST'
    else:
        # Use farm fingerprint just like in BigQuery
        x = np.abs(np.uint64(farmhash.fingerprint64(fl_date_str)).astype('int64') % 100)
        if x < 95:
            data_split = 'TRAIN'
        else:
            data_split = 'VALIDATE'
    return data_split


def to_datetime(event_time):
    if isinstance(event_time, str):
        # In BigQuery, this is a datetime.datetime.  In JSON, it's a string
        # sometimes it has a T separating the date, sometimes it doesn't
        # Handle all the possibilities
        event_time = dt.datetime.strptime(event_time.replace('T', ' '), DATETIME_FORMAT)
    return event_time


def approx_miles_between(lat1, lon1, lat2, lon2):
    # convert to radians
    lat1 = float(lat1) * np.pi / 180.0
    lat2 = float(lat2) * np.pi / 180.0
    lon1 = float(lon1) * np.pi / 180.0
    lon2 = float(lon2) * np.pi / 180.0

    # apply Haversine formula
    d_lat = lat2 - lat1
    d_lon = lon2 - lon1
    a = (pow(np.sin(d_lat / 2), 2) +
         pow(np.sin(d_lon / 2), 2) *
         np.cos(lat1) * np.cos(lat2));
    c = 2 * np.arcsin(np.sqrt(a))
    return float(6371 * c * 0.621371)  # miles


def create_features_and_label(event, for_training):
    try:
        model_input = {}

        if for_training:
            model_input.update({
                'ontime': 1.0 if float(event['ARR_DELAY'] or 0) < 15 else 0,
            })

        # features for both training and prediction
        model_input.update({
            # same as in ch9
            'dep_delay': event['DEP_DELAY'],
            'taxi_out': event['TAXI_OUT'],
            # distance is not in wheelsoff
            'distance': approx_miles_between(event['DEP_AIRPORT_LAT'], event['DEP_AIRPORT_LON'],
                                             event['ARR_AIRPORT_LAT'], event['ARR_AIRPORT_LON']),
            'origin': event['ORIGIN'],
            'dest': event['DEST'],
            'dep_hour': to_datetime(event['DEP_TIME']).hour,
            'is_weekday': 1.0 if to_datetime(event['DEP_TIME']).isoweekday() < 6 else 0.0,
            'carrier': event['UNIQUE_CARRIER'],
            'dep_airport_lat': event['DEP_AIRPORT_LAT'],
            'dep_airport_lon': event['DEP_AIRPORT_LON'],
            'arr_airport_lat': event['ARR_AIRPORT_LAT'],
            'arr_airport_lon': event['ARR_AIRPORT_LON'],
            # newly computed averages
            'avg_dep_delay': event['AVG_DEP_DELAY'],
            'avg_taxi_out': event['AVG_TAXI_OUT'],

        })

        if for_training:
            model_input.update({
                # training data split
                'data_split': get_data_split(event['FL_DATE'])
            })
        else:
            model_input.update({
                # prediction output should include timestamp
                'event_time': event['WHEELS_OFF']
            })

        yield model_input
    except Exception as e:
        # if any key is not present, don't use for training
        logging.warning('Ignoring {} because: {}'.format(event, e), exc_info=True)
        pass


def compute_mean(events, col_name):
    values = [float(event[col_name]) for event in events if col_name in event and event[col_name]]
    return float(np.mean(values)) if len(values) > 0 else None


def add_stats(element, window=beam.DoFn.WindowParam):
    # result of a group-by, so this will be called once for each airport and window
    # all averages here are by airport
    airport = element[0]
    events = element[1]

    # how late are flights leaving?
    avg_dep_delay = compute_mean(events, 'DEP_DELAY')
    avg_taxiout = compute_mean(events, 'TAXI_OUT')

    # remember that an event will be present for 60 minutes, but we want to emit
    # it only if it has just arrived (if it is within 5 minutes of the start of the window)
    emit_end_time = window.start + WINDOW_EVERY
    for event in events:
        event_time = to_datetime(event['WHEELS_OFF']).timestamp()
        if event_time < emit_end_time:
            event_plus_stat = event.copy()
            event_plus_stat['AVG_DEP_DELAY'] = avg_dep_delay
            event_plus_stat['AVG_TAXI_OUT'] = avg_taxiout
            yield event_plus_stat


def assign_timestamp(event):
    try:
        event_time = to_datetime(event['WHEELS_OFF'])
        yield beam.window.TimestampedValue(event, event_time.timestamp())
    except:
        pass


def is_normal_operation(event):
    for flag in ['CANCELLED', 'DIVERTED']:
        if flag in event:
            s = str(event[flag]).lower()
            if s == 'true':
                return False;  # cancelled or diverted
    return True  # normal operation


def transform_events_to_features(events, for_training=True):
    # events are assigned the time at which predictions will have to be made -- the wheels off time
    events = events | 'assign_time' >> beam.FlatMap(assign_timestamp)
    events = events | 'remove_cancelled' >> beam.Filter(is_normal_operation)

    # compute stats by airport, and add to events
    features = (
            events
            | 'window' >> beam.WindowInto(beam.window.SlidingWindows(WINDOW_DURATION, WINDOW_EVERY))
            | 'by_airport' >> beam.Map(lambda x: (x['ORIGIN'], x))
            | 'group_by_airport' >> beam.GroupByKey()
            | 'events_and_stats' >> beam.FlatMap(add_stats)
            | 'events_to_features' >> beam.FlatMap(lambda x: create_features_and_label(x, for_training))
    )

    return features
