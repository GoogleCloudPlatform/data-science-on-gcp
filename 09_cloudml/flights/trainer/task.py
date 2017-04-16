# Copyright 2017 Google Inc. All Rights Reserved.
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

"""Example implementation of code to run on the Cloud ML service.
"""

import argparse
import model
import os

import tensorflow as tf
from tensorflow.contrib.learn.python.learn import learn_runner

if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--traindata',
      help='Training data can have wildcards',
      required=True
  )
  parser.add_argument(
      '--evaldata',
      help='Training data can have wildcards',
      required=True
  )
  parser.add_argument(
      '--job-dir',
      help='this model ignores this field, but it is required by gcloud',
      default='./junk'
  )
  parser.add_argument(
      '--output_dir',
      help='Output directory',
      required=True
  )

  # parse args
  args = parser.parse_args()
  arguments = args.__dict__

  # unused args provided by service
  arguments.pop('job-dir', None)
  arguments.pop('job_dir', None)
  output_dir = arguments.pop('output_dir')

  # run
  tf.logging.set_verbosity(tf.logging.INFO)
  learn_runner.run(model.make_experiment_fn(**arguments), output_dir)
