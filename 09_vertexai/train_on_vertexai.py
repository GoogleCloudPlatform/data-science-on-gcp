# Copyright 2017-2021 Google Inc. All Rights Reserved.
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

import argparse
import logging
from datetime import datetime
import tensorflow as tf

from google.cloud import aiplatform
from google.cloud.aiplatform import gapic as aip

ENDPOINT_NAME = 'flights'

if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--bucket',
        help='Data will be read from gs://BUCKET/ch9/data and checkpoints will be in gs://BUCKET/ch9/trained_model',
        required=True
    )
    parser.add_argument(
        '--region',
        help='Where to run the trainer',
        default='us-central1'
    )
    parser.add_argument(
        '--project',
        help='Project to be billed',
        required=True
    )
    parser.add_argument(
        '--develop',
        help='Train on a small subset in development',
        dest='develop',
        action='store_true')
    parser.set_defaults(develop=False)

    # parse args
    logging.getLogger().setLevel(logging.INFO)
    args = parser.parse_args().__dict__
    BUCKET = args['bucket']
    PROJECT = args['project']
    REGION = args['region']
    DEVELOP_MODE = args['develop']
    TIMESTAMP = datetime.now().strftime("%Y%m%d%H%M%S")

    aiplatform.init(project=PROJECT, location=REGION, staging_bucket='gs://{}'.format(BUCKET))

    # Set up training and deployment infra
    TF_VERSION = '2-' + tf.__version__[2:3]
    TRAIN_IMAGE = "us-docker.pkg.dev/vertex-ai/training/tf-gpu.{}:latest".format(TF_VERSION)
    DEPLOY_IMAGE = "us-docker.pkg.dev/vertex-ai/prediction/tf2-cpu.{}:latest".format(TF_VERSION)

    # create dataset
    dataset = aiplatform.TabularDataset.create(
        display_name='data-{}'.format(ENDPOINT_NAME),
        gcs_source=['gs://{}/ch9/data/all.csv'.format(BUCKET)]
    )

    # train
    MODEL_DISPLAY_NAME = '{}-{}'.format(ENDPOINT_NAME, TIMESTAMP)
    job = aiplatform.CustomTrainingJob(
        display_name='train-{}'.format(MODEL_DISPLAY_NAME),
        script_path="model.py",
        container_uri=TRAIN_IMAGE,
        requirements=[],  # any extra Python packages
        model_serving_container_image_uri=DEPLOY_IMAGE
    )

    model_args = [
            '--bucket', BUCKET,
    ]
    if DEVELOP_MODE:
        model_args += ['--develop']
    model = job.run(
        dataset=dataset,
        # See https://googleapis.dev/python/aiplatform/latest/aiplatform.html#
        predefined_split_column_name='data_split',
        model_display_name=MODEL_DISPLAY_NAME,
        args=model_args,
        replica_count=1,
        machine_type='n1-standard-4',
        # See https://cloud.google.com/vertex-ai/docs/general/locations#accelerators
        accelerator_type=aip.AcceleratorType.NVIDIA_TESLA_T4.name,
        accelerator_count=1,
        sync=DEVELOP_MODE
    )

    # create endpoint if it doesn't already exist
    endpoints = aiplatform.Endpoint.list(
        filter='display_name="{}"'.format(ENDPOINT_NAME),
        order_by='create_time desc',
        project=PROJECT, location=REGION,
    )
    if len(endpoints) > 0:
        endpoint = endpoints[0]  # most recently created
    else:
        endpoint = aiplatform.Endpoint.create(
            display_name=ENDPOINT_NAME, project=PROJECT, location=REGION,
            sync=DEVELOP_MODE
        )

    # deploy
    model.deploy(
        endpoint=endpoint,
        traffic_split={"0": 100},
        machine_type='n1-standard-2',
        min_replica_count=1,
        max_replica_count=1,
        sync=DEVELOP_MODE
    )

    if DEVELOP_MODE:
        model.wait()


