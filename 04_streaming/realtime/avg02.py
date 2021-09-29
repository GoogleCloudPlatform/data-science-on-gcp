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
import logging
import json
import numpy as np

DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S'


def compute_stats(airport, events):
    arrived = [event['ARR_DELAY'] for event in events if event['EVENT_TYPE'] == 'arrived']
    avg_arr_delay = float(np.mean(arrived)) if len(arrived) > 0 else None

    departed = [event['DEP_DELAY'] for event in events if event['EVENT_TYPE'] == 'departed']
    avg_dep_delay = float(np.mean(departed)) if len(departed) > 0 else None

    num_flights = len(events)
    start_time = min([event['EVENT_TIME'] for event in events])
    latest_time = max([event['EVENT_TIME'] for event in events])

    return {
        'AIRPORT': airport,
        'AVG_ARR_DELAY': avg_arr_delay,
        'AVG_DEP_DELAY': avg_dep_delay,
        'NUM_FLIGHTS': num_flights,
        'START_TIME': start_time,
        'END_TIME': latest_time
    }


def by_airport(event):
    if event['EVENT_TYPE'] == 'departed':
        return event['ORIGIN'], event
    else:
        return event['DEST'], event


def run(project, bucket, region):
    argv = [
        '--project={0}'.format(project),
        '--job_name=ch04avgdelay',
        '--streaming',
        '--save_main_session',
        '--staging_location=gs://{0}/flights/staging/'.format(bucket),
        '--temp_location=gs://{0}/flights/temp/'.format(bucket),
        '--autoscaling_algorithm=THROUGHPUT_BASED',
        '--max_num_workers=8',
        '--region={}'.format(region),
        '--runner=DirectRunner'
    ]

    with beam.Pipeline(argv=argv) as pipeline:
        events = {}

        for event_name in ['arrived', 'departed']:
            topic_name = "projects/{}/topics/{}".format(project, event_name)

            events[event_name] = (pipeline
                                  | 'read:{}'.format(event_name) >> beam.io.ReadFromPubSub(
                                                topic=topic_name, timestamp_attribute='EventTimeStamp')
                                  | 'parse:{}'.format(event_name) >> beam.Map(lambda s: json.loads(s))
                                  )

        all_events = (events['arrived'], events['departed']) | beam.Flatten()

        stats = (all_events
                 | 'byairport' >> beam.Map(by_airport)
                 | 'window' >> beam.WindowInto(beam.window.SlidingWindows(60 * 60, 5 * 60))
                 | 'group' >> beam.GroupByKey()
                 | 'stats' >> beam.Map(lambda x: compute_stats(x[0], x[1]))
        )

        stats_schema = ','.join(['AIRPORT:string,AVG_ARR_DELAY:float,AVG_DEP_DELAY:float',
                                 'NUM_FLIGHTS:int64,START_TIME:timestamp,END_TIME:timestamp'])
        (stats
         | 'bqout' >> beam.io.WriteToBigQuery(
                    'dsongcp.streaming_delays', schema=stats_schema,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
                )
         )


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Run pipeline on the cloud')
    parser.add_argument('-p', '--project', help='Unique project ID', required=True)
    parser.add_argument('-b', '--bucket', help='Bucket where gs://BUCKET/flights/airports/airports.csv.gz exists',
                        required=True)
    parser.add_argument('-r', '--region',
                        help='Region in which to run the Dataflow job. Choose the same region as your bucket.',
                        required=True)

    args = vars(parser.parse_args())

    run(project=args['project'], bucket=args['bucket'], region=args['region'])
