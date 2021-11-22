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
import os
import numpy as np
import farmhash  # pip install pyfarmhash
import json

DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
WINDOW_DURATION = 60 * 60
WINDOW_EVERY = 5 * 60
CSV_HEADER = 'ontime,dep_delay,taxi_out,distance,origin,dest,dep_hour,is_weekday,carrier,dep_airport_lat,dep_airport_lon,arr_airport_lat,arr_airport_lon,avg_dep_delay,avg_taxi_out,data_split'


def get_data_split(col):
    # Use farm fingerprint just like in BigQuery
    x = np.abs(np.uint64(farmhash.fingerprint64(str(col))).astype('int64') % 100)
    if x < 60:
        data_split = 'TRAIN'
    elif x < 80:
        data_split = 'VALIDATE'
    else:
        data_split = 'TEST'
    return data_split


def to_datetime(event_time):
    if isinstance(event_time, str):
        # In BigQuery, this is a datetime.datetime.  In JSON, it's a string
        event_time = dt.datetime.strptime(event_time, DATETIME_FORMAT)
    return event_time


def create_features_and_label(event):
    try:
        model_input = {
            'ontime': 1.0 if float(event['ARR_DELAY']) < 15 else 0,
            # same as in ch9
            'dep_delay': event['DEP_DELAY'],
            'taxi_out': event['TAXI_OUT'],
            # 'distance': event['DISTANCE'],  # distance is not in wheelsoff
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
            # training data split
            'data_split': get_data_split(event['FL_DATE'])
        }
        yield model_input
    except Exception as e:
        # if any key is not present, don't use for training
        logging.warning('Ignoring {} because: {}'.format(event, e), exc_info=True)
        pass


def compute_mean(events, col_name):
    values = [float(event[col_name]) for event in events]
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


def run(project, bucket, region, input):
    if input == 'local':
        logging.info('Running locally on small extract')
        argv = [
            '--runner=DirectRunner'
        ]
        flights_output = '/tmp/'
    else:
        logging.info('Running in the cloud on full dataset input={}'.format(input))
        argv = [
            '--project={0}'.format(project),
            '--job_name=ch10traindata',
            '--save_main_session',
            '--staging_location=gs://{0}/flights/staging/'.format(bucket),
            '--temp_location=gs://{0}/flights/temp/'.format(bucket),
            '--setup_file=./setup.py',
            '--autoscaling_algorithm=THROUGHPUT_BASED',
            '--max_num_workers=8',
            '--region={}'.format(region),
            '--runner=DataflowRunner'
        ]
        flights_output = 'gs://{}/flights/ch10/'.format(bucket)

    with beam.Pipeline(argv=argv) as pipeline:

        # read the event stream
        if input == 'local':
            input_file = './alldata_sample.json'
            logging.info("Reading from {} ... Writing to {}".format(input_file, flights_output))
            events = (
                    pipeline
                    | 'read_input' >> beam.io.ReadFromText(input_file)
                    | 'parse_input' >> beam.Map(lambda line: json.loads(line))
            )
        elif input == 'bigquery':
            input_table = 'dsongcp.flights_tzcorr'
            logging.info("Reading from {} ... Writing to {}".format(input_table, flights_output))
            events = (
                    pipeline
                    | 'read_input' >> beam.io.ReadFromBigQuery(table=input_table)
            )
        else:
            logging.error("Unknown input type {}".format(input))
            return

        # events are assigned the time at which predictions will have to be made -- the wheels off time
        events = events | 'assign_time' >> beam.FlatMap(assign_timestamp)

        # compute stats by airport, and add to events
        features = (
                events
                | 'window' >> beam.WindowInto(beam.window.SlidingWindows(WINDOW_DURATION, WINDOW_EVERY))
                | 'by_airport' >> beam.Map(lambda x: (x['ORIGIN'], x))
                | 'group_by_airport' >> beam.GroupByKey()
                | 'events_and_stats' >> beam.FlatMap(add_stats)
                | 'events_to_features' >> beam.FlatMap(create_features_and_label)
        )

        # write out
        for split in ['ALL', 'TRAIN', 'VALIDATE', 'TEST']:
            feats = features
            if split != 'ALL':
                feats = feats | 'only_{}'.format(split) >> beam.Filter(lambda f: f['data_split'] == split)
            (
                feats
                | '{}_to_string'.format(split) >> beam.Map(lambda f: ','.join([str(x) for x in f.values()]))
                | '{}_to_gcs'.format(split) >> beam.io.textio.WriteToText(os.path.join(flights_output, split.lower()),
                                                                          file_name_suffix='.csv', header=CSV_HEADER)
            )


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Create training CSV file that includes time-aggregate features')
    parser.add_argument('-p', '--project', help='Project to be billed for Dataflow job. Omit if running locally.')
    parser.add_argument('-b', '--bucket', help='Training data will be written to gs://BUCKET/flights/ch10/')
    parser.add_argument('-r', '--region', help='Region to run Dataflow job. Choose the same region as your bucket.')
    parser.add_argument('-i', '--input', help='local OR bigquery', required=True)

    logging.getLogger().setLevel(logging.INFO)
    args = vars(parser.parse_args())

    if args['input'] != 'local':
        if not args['bucket'] or not args['project'] or not args['region']:
            print("Project, Bucket, Region are needed in order to run on the cloud on full dataset.")
            parser.print_help()
            parser.exit()

    run(project=args['project'], bucket=args['bucket'], region=args['region'], input=args['input'])
