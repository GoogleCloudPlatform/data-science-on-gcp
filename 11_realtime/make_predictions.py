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
import os

from flightstxf import flights_transforms as ftxf


CSV_HEADER = 'event_time,dep_delay,taxi_out,distance,origin,dest,dep_hour,is_weekday,carrier,dep_airport_lat,dep_airport_lon,arr_airport_lat,arr_airport_lon,avg_dep_delay,avg_taxi_out,prob_ontime'


# class FlightsModelSharedInvoker(beam.DoFn):
#     # https://beam.apache.org/releases/pydoc/2.24.0/apache_beam.utils.shared.html
#     def __init__(self, shared_handle):
#         self._shared_handle = shared_handle
#
#     def process(self, input_data):
#         def create_endpoint():
#             from google.cloud import aiplatform
#             endpoint_name = 'flights-ch10'
#             endpoints = aiplatform.Endpoint.list(
#                 filter='display_name="{}"'.format(endpoint_name),
#                 order_by='create_time desc'
#             )
#             if len(endpoints) == 0:
#                 raise EnvironmentError("No endpoint named {}".format(endpoint_name))
#             logging.info("Found endpoint {}".format(endpoints[0]))
#             return endpoints[0]
#
#         # get already created endpoint if possible
#         endpoint = self._shared_handle.acquire(create_endpoint)
#
#         # call predictions and pull out probability
#         logging.info("Invoking ML model on {} flights".format(len(input_data)))
#         predictions = endpoint.predict(input_data).predictions
#         for idx, input_instance in enumerate(input_data):
#             result = input_instance.copy()
#             result['prob_ontime'] = predictions[idx][0]
#             yield result


class FlightsModelInvoker(beam.DoFn):
    def __init__(self):
        self.endpoint = None

    def setup(self):
        from google.cloud import aiplatform
        endpoint_name = 'flights-ch11'
        endpoints = aiplatform.Endpoint.list(
            filter='display_name="{}"'.format(endpoint_name),
            order_by='create_time desc'
        )
        if len(endpoints) == 0:
            raise EnvironmentError("No endpoint named {}".format(endpoint_name))
        logging.info("Found endpoint {}".format(endpoints[0]))
        self.endpoint = endpoints[0]

    def process(self, input_data):
        # call predictions and pull out probability
        logging.info("Invoking ML model on {} flights".format(len(input_data)))
        # drop inputs not needed by model
        features = [x.copy() for x in input_data]
        for f in features:
            f.pop('event_time')
        # call model
        predictions = self.endpoint.predict(features).predictions
        for idx, input_instance in enumerate(input_data):
            result = input_instance.copy()
            result['prob_ontime'] = predictions[idx][0]
            yield result


def run(project, bucket, region, source, sink):
    if source == 'local':
        logging.info('Running locally on small extract')
        argv = [
            '--project={0}'.format(project),
            '--runner=DirectRunner'
        ]
        flights_output = '/tmp/predictions'
    else:
        logging.info('Running in the cloud on full dataset input={}'.format(source))
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
        if source == 'pubsub':
            logging.info("Turning on streaming. Cancel the pipeline from GCP console")
            argv += ['--streaming']
        flights_output = 'gs://{}/flights/ch11/predictions'.format(bucket)

    with beam.Pipeline(argv=argv) as pipeline:

        # read the event stream
        if source == 'local':
            input_file = './simevents_sample.json'
            logging.info("Reading from {} ... Writing to {}".format(input_file, flights_output))
            events = (
                    pipeline
                    | 'read_input' >> beam.io.ReadFromText(input_file)
                    | 'parse_input' >> beam.Map(lambda line: json.loads(line))
            )
        elif source == 'bigquery':
            input_query = ("SELECT EVENT_DATA FROM dsongcp.flights_simevents " +
                           "WHERE EVENT_TIME BETWEEN '2015-03-01' AND '2015-03-02'")
            logging.info("Reading from {} ... Writing to {}".format(input_query, flights_output))
            events = (
                    pipeline
                    | 'read_input' >> beam.io.ReadFromBigQuery(query=input_query, use_standard_sql=True)
                    | 'parse_input' >> beam.Map(lambda row: json.loads(row['EVENT_DATA']))
            )
        elif source == 'pubsub':
            input_topic = "projects/{}/topics/wheelsoff".format(project)
            logging.info("Reading from {} ... Writing to {}".format(input_topic, flights_output))
            events = (
                    pipeline
                    | 'read_input' >> beam.io.ReadFromPubSub(topic=input_topic,
                                                             timestamp_attribute='EventTimeStamp')
                    | 'parse_input' >> beam.Map(lambda s: json.loads(s))
            )
        else:
            logging.error("Unknown input type {}".format(source))
            return

        # events -> features.  See ./flights_transforms.py for the code shared between training & prediction
        features = ftxf.transform_events_to_features(events, for_training=False)

        # call model endpoint
        # shared_handle = beam.utils.shared.Shared()
        preds = (
                features
                | 'into_global' >> beam.WindowInto(beam.window.GlobalWindows())
                | 'batch_instances' >> beam.BatchElements(min_batch_size=1, max_batch_size=64)
                | 'model_predict' >> beam.ParDo(FlightsModelInvoker())
        )

        # write it out
        if sink == 'file':
            (preds
             | 'to_string' >> beam.Map(lambda f: ','.join([str(x) for x in f.values()]))
             | 'to_gcs' >> beam.io.textio.WriteToText(flights_output,
                                                      file_name_suffix='.csv', header=CSV_HEADER,
                                                      # workaround b/207384805
                                                      num_shards=1)
             )
        elif sink == 'bigquery':
            preds_schema = ','.join([
                'event_time:timestamp',
                'prob_ontime:float',
                'dep_delay:float',
                'taxi_out:float',
                'distance:float',
                'origin:string',
                'dest:string',
                'dep_hour:integer',
                'is_weekday:integer',
                'carrier:string',
                'dep_airport_lat:float,dep_airport_lon:float',
                'arr_airport_lat:float,arr_airport_lon:float',
                'avg_dep_delay:float',
                'avg_taxi_out:float',
            ])
            (preds
             | 'to_bigquery' >> beam.io.WriteToBigQuery(
                        'dsongcp.streaming_preds', schema=preds_schema,
                        # write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                        method='STREAMING_INSERTS'
                    )
             )
        else:
            logging.error("Unknown output type {}".format(sink))
            return


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Create training CSV file that includes time-aggregate features')
    parser.add_argument('-p', '--project', help='Project to be billed for Dataflow/BigQuery', required=True)
    parser.add_argument('-b', '--bucket', help='data will be read from written to gs://BUCKET/flights/ch11/')
    parser.add_argument('-r', '--region', help='Region to run Dataflow job. Choose the same region as your bucket.')
    parser.add_argument('-i', '--input', help='local, bigquery OR pubsub', required=True)
    parser.add_argument('-o', '--output', help='file, bigquery OR bigtable', default='file')

    logging.getLogger().setLevel(logging.INFO)
    args = vars(parser.parse_args())

    if args['input'] != 'local':
        if not args['bucket'] or not args['project'] or not args['region']:
            print("Project, Bucket, Region are needed in order to run on the cloud on full dataset.")
            parser.print_help()
            parser.exit()

    run(project=args['project'], bucket=args['bucket'], region=args['region'],
        source=args['input'], sink=args['output'])
