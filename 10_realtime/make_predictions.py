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
import json

DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S'
WINDOW_DURATION = 60 * 60
WINDOW_EVERY = 5 * 60


def create_features(event):
    model_input = {
        # same as in ch9
        'dep_delay': event['DEP_DELAY'],
        'taxi_out': event['TAXI_OUT'],
        # 'distance': event['DISTANCE'],  # distance is not in wheelsoff
        'origin': event['ORIGIN'],
        'dest': event['DEST'],
        'dep_hour': dt.datetime.strptime(event['DEP_TIME'], DATETIME_FORMAT).hour,
        'is_weekday': 1.0 if dt.datetime.strptime(event['DEP_TIME'], DATETIME_FORMAT).isoweekday() < 6 else 0.0,
        'carrier': event['UNIQUE_CARRIER'],
        'dep_airport_lat': event['DEP_AIRPORT_LAT'],
        'dep_airport_lon': event['DEP_AIRPORT_LON'],
        'arr_airport_lat': event['ARR_AIRPORT_LAT'],
        'arr_airport_lon': event['ARR_AIRPORT_LON'],
        # newly computed averages
        'avg_dep_delay': event['AVG_DEP_DELAY'],
        'avg_taxi_out': event['AVG_TAXI_OUT'],
    }
    return model_input


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
        event_time = dt.datetime.strptime(event['EVENT_TIME'], DATETIME_FORMAT).timestamp()
        if event_time < emit_end_time:
            event_plus_stat = event.copy()
            event_plus_stat['AVG_DEP_DELAY'] = avg_dep_delay
            event_plus_stat['AVG_TAXI_OUT'] = avg_taxiout
            yield event_plus_stat


def run(project, bucket, region, input):
    if input == 'local':
        logging.info('Running locally on small extract')
        argv = [
            '--runner=DirectRunner'
        ]
        flights_output = '/tmp/predictions'
    else:
        logging.info('Running in the cloud on full dataset input={}'.format(input))
        argv = [
            '--project={0}'.format(project),
            '--job_name=ch10predictions',
            '--save_main_session',
            '--staging_location=gs://{0}/flights/staging/'.format(bucket),
            '--temp_location=gs://{0}/flights/temp/'.format(bucket),
            '--setup_file=./setup.py',
            '--autoscaling_algorithm=THROUGHPUT_BASED',
            '--max_num_workers=8',
            '--region={}'.format(region),
            '--runner=DataflowRunner'
        ]
        flights_output = 'gs://{}/flights/ch10/predictions'.format(bucket)

    with beam.Pipeline(argv=argv) as pipeline:

        # read the event stream
        if input == 'local':
            input_file = './simevents_sample.json'
            logging.info("Reading from {} ... Writing to {}".format(input_file, flights_output))
            events = (
                    pipeline
                    | 'read_input' >> beam.io.ReadFromText(input_file)
                    | 'parse_input' >> beam.Map(lambda line: json.loads(line))
            )
        elif input == 'bigquery':
            input_query = 'SELECT EVENT_DATA FROM dsongcp.flights_simevents'
            logging.info("Reading from {} ... Writing to {}".format(input_query, flights_output))
            events = (
                    pipeline
                    | 'read_input' >> beam.io.ReadFromBigQuery(query=input_query, use_standard_sql=True)
                    | 'parse_input' >> beam.Map(lambda row: json.loads(row['EVENT_DATA']))
            )
        elif input == 'pubsub':
            input_topic = "projects/{}/topics/wheelsoff".format(project)
            logging.info("Reading from {} ... Writing to {}".format(input_topic, flights_output))
            events = (
                    pipeline
                    | 'read_input' >> beam.io.ReadFromPubSub(topic=input_topic,
                                                             timestamp_attribute='EventTimeStamp')
                    | 'parse_input' >> beam.Map(lambda s: json.loads(s))
            )
        else:
            logging.error("Unknown input type {}".format(input))
            return

        # assign the correct timestamp to the events
        events = (
                events
                | 'assign_time' >> beam.Map(lambda event:
                                            beam.window.TimestampedValue(event,
                                                                         dt.datetime.strptime(event['EVENT_TIME'],
                                                                                              DATETIME_FORMAT).timestamp()))
        )

        # compute stats by airport, and add to events
        features = (
                events
                | 'window' >> beam.WindowInto(beam.window.SlidingWindows(WINDOW_DURATION, WINDOW_EVERY))
                | 'by_airport' >> beam.Map(lambda x: (x['ORIGIN'], x))
                | 'group_by_airport' >> beam.GroupByKey()
                | 'events_and_stats' >> beam.FlatMap(add_stats)
                | 'events_to_features' >> beam.Map(create_features)
        )

        # write it out
        (features
         # FIXME: make call to prediction endpoint, and add predicted ontime probability
         | 'to_string' >> beam.Map(lambda x: json.dumps(x))
         | 'flights_to_gcs' >> beam.io.textio.WriteToText(flights_output)
         )


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Create training CSV file that includes time-aggregate features')
    parser.add_argument('-p', '--project', help='Project to be billed for Dataflow job. Omit if running locally.')
    parser.add_argument('-b', '--bucket', help='Training data will be written to gs://BUCKET/flights/ch10/')
    parser.add_argument('-r', '--region', help='Region to run Dataflow job. Choose the same region as your bucket.')
    parser.add_argument('-i', '--input', help='local, bigquery OR pubsub', required=True)

    logging.getLogger().setLevel(logging.INFO)
    args = vars(parser.parse_args())

    if args['input'] != 'local':
        if not args['bucket'] or not args['project'] or not args['region']:
            print("Project, Bucket, Region are needed in order to run on the cloud on full dataset.")
            parser.print_help()
            parser.exit()

    run(project=args['project'], bucket=args['bucket'], region=args['region'], input=args['input'])
