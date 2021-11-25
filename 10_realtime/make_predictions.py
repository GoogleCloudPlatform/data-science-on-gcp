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

from .flightstxf import flights_transforms as ftxf


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

        # events -> features.  See ./flights_transforms.py for the code shared between training & prediction
        features = ftxf.transform_events_to_features(events)

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
