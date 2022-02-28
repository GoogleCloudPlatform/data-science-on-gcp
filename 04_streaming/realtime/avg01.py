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
import json


def run(project, bucket, region):
    argv = [
        '--project={0}'.format(project),
        '--job_name=ch04avgdelay',
        '--streaming',
        '--save_main_session',
        '--staging_location=gs://{0}/flights/staging/'.format(bucket),
        '--temp_location=gs://{0}/flights/temp/'.format(bucket),
        '--setup_file=./setup.py',
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
                                  | 'read:{}'.format(event_name) >> beam.io.ReadFromPubSub(topic=topic_name)
                                  | 'parse:{}'.format(event_name) >> beam.Map(lambda s: json.loads(s))
                                  )

        all_events = (events['arrived'], events['departed']) | beam.Flatten()

        flights_schema = ','.join([
            'FL_DATE:date,UNIQUE_CARRIER:string,ORIGIN_AIRPORT_SEQ_ID:string,ORIGIN:string',
            'DEST_AIRPORT_SEQ_ID:string,DEST:string,CRS_DEP_TIME:timestamp,DEP_TIME:timestamp',
            'DEP_DELAY:float,TAXI_OUT:float,WHEELS_OFF:timestamp,WHEELS_ON:timestamp,TAXI_IN:float',
            'CRS_ARR_TIME:timestamp,ARR_TIME:timestamp,ARR_DELAY:float,CANCELLED:boolean',
            'DIVERTED:boolean,DISTANCE:float',
            'DEP_AIRPORT_LAT:float,DEP_AIRPORT_LON:float,DEP_AIRPORT_TZOFFSET:float',
            'ARR_AIRPORT_LAT:float,ARR_AIRPORT_LON:float,ARR_AIRPORT_TZOFFSET:float'])
        events_schema = ','.join([flights_schema, 'EVENT_TYPE:string,EVENT_TIME:timestamp'])

        schema = events_schema

        (all_events
         | 'bqout' >> beam.io.WriteToBigQuery(
                    'dsongcp.streaming_events', schema=schema,
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
