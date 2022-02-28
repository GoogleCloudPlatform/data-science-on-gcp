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
import numpy as np

NUM_PARTITIONS = 1000

def get_category(hour):
    if hour < 6 or hour > 20:
        return [1, 0, 0]  # night
    if hour < 10:
        return [0, 1, 0]  # morning
    if hour < 17:
        return [0, 0, 1]  # mid-day
    else:
        return [0, 0, 0]  # evening


def get_local_hour(timestamp, correction):
    import datetime
    TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
    timestamp = timestamp.replace('T', ' ')  # incase different
    t = datetime.datetime.strptime(timestamp, TIME_FORMAT)
    d = datetime.timedelta(seconds=correction)
    t = t + d
    # return [t.hour]    # raw
    # theta = np.radians(360 * t.hour / 24.0)  # von-Miyes
    # return [np.sin(theta), np.cos(theta)]
    return get_category(t.hour)  # bucketize


def eval(labelpred):
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

    totsqe = labelpred.map(
        lambda data: (data[0] - data[1]) * (data[0] - data[1])
    ).sum()
    rmse = np.sqrt(totsqe / float(cancel.count() + nocancel.count()))

    return {
        'rmse': rmse,
        'total_cancel': cancel.count(),
        'correct_cancel': float(corr_cancel) / cancel_denom,
        'total_noncancel': nocancel.count(),
        'correct_noncancel': float(corr_nocancel) / nocancel_denom
    }


def run_experiment(BUCKET, SCALE_AND_CLIP, WITH_TIME, WITH_ORIGIN):
    # Create spark session
    sc = SparkContext('local', 'experimentation')
    spark = SparkSession \
        .builder \
        .appName("Logistic regression w/ Spark ML") \
        .getOrCreate()

    # read dataset
    traindays = spark.read \
        .option("header", "true") \
        .csv('gs://{}/flights/trainday.csv'.format(BUCKET))
    traindays.createOrReplaceTempView('traindays')

    #inputs = 'gs://{}/flights/tzcorr/all_flights-00000-*'.format(BUCKET) # 1/30th
    inputs = 'gs://{}/flights/tzcorr/all_flights-*'.format(BUCKET)  # FULL
    flights = spark.read.json(inputs)

    # this view can now be queried
    flights.createOrReplaceTempView('flights')

    # separate training and validation data
    from pyspark.sql.functions import rand
    SEED=13
    traindays = traindays.withColumn("holdout", rand(SEED) > 0.8)  # 80% of data is for training
    traindays.createOrReplaceTempView('traindays')

    # logistic regression
    trainquery = """
    SELECT
        ORIGIN, DEP_DELAY, TAXI_OUT, ARR_DELAY, DISTANCE, DEP_TIME, DEP_AIRPORT_TZOFFSET
    FROM flights f
    JOIN traindays t
    ON f.FL_DATE == t.FL_DATE
    WHERE
      t.is_train_day == 'True' AND
      t.holdout == False AND
      f.CANCELLED == 'False' AND 
      f.DIVERTED == 'False'
    """
    traindata = spark.sql(trainquery).repartition(NUM_PARTITIONS)

    def to_example(fields):
        features = [
            fields['DEP_DELAY'],
            fields['DISTANCE'],
            fields['TAXI_OUT'],
        ]

        if SCALE_AND_CLIP:
            def clip(x):
                if x < -1:
                    return -1
                if x > 1:
                    return 1
                return x
            features = [
                clip(float(fields['DEP_DELAY']) / 30),
                clip((float(fields['DISTANCE']) / 1000) - 1),
                clip((float(fields['TAXI_OUT']) / 10) - 1),
            ]

        if WITH_TIME:
            features.extend(
                get_local_hour(fields['DEP_TIME'], fields['DEP_AIRPORT_TZOFFSET']))

        if WITH_ORIGIN:
            features.extend(fields['origin_onehot'])

        return LabeledPoint(
              float(fields['ARR_DELAY'] < 15), #ontime
              features)

    def add_origin(df, trained_model=None):
        from pyspark.ml.feature import OneHotEncoder, StringIndexer
        if not trained_model:
            indexer = StringIndexer(inputCol='ORIGIN', outputCol='origin_index')
            trained_model = indexer.fit(df)
        indexed = trained_model.transform(df)
        encoder = OneHotEncoder(inputCol='origin_index', outputCol='origin_onehot')
        return trained_model, encoder.fit(indexed).transform(indexed)

    if WITH_ORIGIN:
        index_model, traindata = add_origin(traindata)

    examples = traindata.rdd.map(to_example)
    lrmodel = LogisticRegressionWithLBFGS.train(examples, intercept=True)
    lrmodel.clearThreshold()  # return probabilities

    # save model
    MODEL_FILE='gs://' + BUCKET + '/flights/sparkmloutput/model'
    lrmodel.save(sc, MODEL_FILE)
    logging.info("Saved trained model to {}".format(MODEL_FILE))

    # evaluate model on the heldout data
    evalquery = trainquery.replace("t.holdout == False", "t.holdout == True")
    evaldata = spark.sql(evalquery).repartition(NUM_PARTITIONS)
    if WITH_ORIGIN:
        evaldata = add_origin(evaldata, index_model)
    examples = evaldata.rdd.map(to_example)
    labelpred = examples.map(lambda p: (p.label, lrmodel.predict(p.features)))


    logging.info(eval(labelpred))



if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Run experiments with different features in Spark')
    parser.add_argument('--bucket', help='GCS bucket to read/write data', required=True)
    parser.add_argument('--debug', dest='debug', action='store_true', help='Specify if you want debug messages')

    args = parser.parse_args()
    if args.debug:
        logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.DEBUG)
    else:
        logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)

    run_experiment(args.bucket, SCALE_AND_CLIP=False, WITH_TIME=False, WITH_ORIGIN=False)

