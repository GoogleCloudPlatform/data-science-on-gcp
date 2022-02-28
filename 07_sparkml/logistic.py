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

from pyspark.mllib.classification import LogisticRegressionWithLBFGS
from pyspark.mllib.regression import LabeledPoint
from pyspark.sql import SparkSession
from pyspark import SparkContext
import logging

def run_logistic(BUCKET):
    # Create spark session
    sc = SparkContext('local', 'logistic')
    spark = SparkSession \
        .builder \
        .appName("Logistic regression w/ Spark ML") \
        .getOrCreate()

    # read dataset
    traindays = spark.read \
        .option("header", "true") \
        .csv('gs://{}/flights/trainday.csv'.format(BUCKET))
    traindays.createOrReplaceTempView('traindays')

    # inputs = 'gs://{}/flights/tzcorr/all_flights-00000-*'.format(BUCKET)  # 1/30th
    inputs = 'gs://{}/flights/tzcorr/all_flights-*'.format(BUCKET)  # FULL
    flights = spark.read.json(inputs)

    # this view can now be queried ...
    flights.createOrReplaceTempView('flights')


    # logistic regression
    trainquery = """
    SELECT
      DEP_DELAY, TAXI_OUT, ARR_DELAY, DISTANCE
    FROM flights f
    JOIN traindays t
    ON f.FL_DATE == t.FL_DATE
    WHERE
      t.is_train_day == 'True' AND
      f.CANCELLED == 'False' AND 
      f.DIVERTED == 'False'
    """
    traindata = spark.sql(trainquery)

    def to_example(fields):
      return LabeledPoint(\
                  float(fields['ARR_DELAY'] < 15), #ontime \
                  [ \
                      fields['DEP_DELAY'], # DEP_DELAY \
                      fields['TAXI_OUT'], # TAXI_OUT \
                      fields['DISTANCE'], # DISTANCE \
                  ])

    examples = traindata.rdd.map(to_example)
    lrmodel = LogisticRegressionWithLBFGS.train(examples, intercept=True)
    lrmodel.setThreshold(0.7)

    # save model
    MODEL_FILE='gs://{}/flights/sparkmloutput/model'.format(BUCKET)
    lrmodel.save(sc, MODEL_FILE)
    logging.info('Logistic regression model saved in {}'.format(MODEL_FILE))

    # evaluate
    testquery = trainquery.replace("t.is_train_day == 'True'","t.is_train_day == 'False'")
    testdata = spark.sql(testquery)
    examples = testdata.rdd.map(to_example)

    # Evaluate model
    lrmodel.clearThreshold() # so it returns probabilities
    labelpred = examples.map(lambda p: (p.label, lrmodel.predict(p.features)))
    logging.info('All flights: {}'.format(eval_model(labelpred)))


    # keep only those examples near the decision threshold
    labelpred = labelpred.filter(lambda data: data[1] > 0.65 and data[1] < 0.75)
    logging.info('Flights near decision threshold: {}'.format(eval_model(labelpred)))

def eval_model(labelpred):
    '''
            data = (label, pred)
                data[0] = label
                data[1] = pred
    '''
    cancel = labelpred.filter(lambda data: data[1] < 0.7)
    nocancel = labelpred.filter(lambda data: data[1] >= 0.7)
    corr_cancel = cancel.filter(lambda data: data[0] == int(data[1] >= 0.7)).count()
    corr_nocancel = nocancel.filter(lambda data: data[0] == int(data[1] >= 0.7)).count()

    cancel_denom = cancel.count()
    nocancel_denom = nocancel.count()
    if cancel_denom == 0:
        cancel_denom = 1
    if nocancel_denom == 0:
        nocancel_denom = 1
    return {
        'total_cancel': cancel.count(),
        'correct_cancel': float(corr_cancel)/cancel_denom,
        'total_noncancel': nocancel.count(),
        'correct_noncancel': float(corr_nocancel)/nocancel_denom
    }


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Run logistic regression in Spark')
    parser.add_argument('--bucket', help='GCS bucket to read/write data', required=True)
    parser.add_argument('--debug', dest='debug', action='store_true', help='Specify if you want debug messages')

    args = parser.parse_args()
    if args.debug:
        logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.DEBUG)
    else:
        logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)

    run_logistic(args.bucket)
