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
import os
import numpy as np
import farmhash  # pip install pyfarmhash
import json

from flightstxf import flights_transforms as ftxf

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
            # '--save_main_session', # not needed as we are running as a package now
            '--staging_location=gs://{0}/flights/staging/'.format(bucket),
            '--temp_location=gs://{0}/flights/temp/'.format(bucket),
            '--setup_file=./setup.py',
            '--autoscaling_algorithm=THROUGHPUT_BASED',
            '--max_num_workers=20',
            '--region={}'.format(region),
            '--runner=DataflowRunner'
        ]
        flights_output = 'gs://{}/ch10/data/'.format(bucket)

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

        # events -> features.  See ./flights_transforms.py for the code shared between training & prediction
        features = ftxf.transform_events_to_features(events)

        # write out
        for split in ['ALL', 'TRAIN', 'VALIDATE', 'TEST']:
            feats = features
            if split != 'ALL':
                feats = feats | 'only_{}'.format(split) >> beam.Filter(lambda f: f['data_split'] == split)
            (
                feats
                | '{}_to_string'.format(split) >> beam.Map(lambda f: ','.join([str(x) for x in f.values()]))
                | '{}_to_gcs'.format(split) >> beam.io.textio.WriteToText(os.path.join(flights_output, split.lower()),
                                                                          file_name_suffix='.csv', header=CSV_HEADER,
                                                                          # workaround b/207384805
                                                                          num_shards=1)
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
