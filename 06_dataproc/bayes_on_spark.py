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

import logging
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
import pyspark.sql.functions as F


def run_bayes(BUCKET):
    spark = SparkSession \
        .builder \
        .appName("Bayes classification using Spark") \
        .getOrCreate()

    # read flights data
    inputs = 'gs://{}/flights/tzcorr/all_flights-*'.format(BUCKET)  # FULL
    flights = spark.read.json(inputs)
    flights.createOrReplaceTempView('flights')

    # which days are training days?
    traindays = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv('gs://{}/flights/trainday.csv'.format(BUCKET))
    traindays.createOrReplaceTempView('traindays')

    # create training dataset
    statement = """
    SELECT
      f.FL_DATE AS date,
      CAST(distance AS FLOAT) AS distance,
      dep_delay,
      IF(arr_delay < 15, 1, 0) AS ontime
    FROM flights f
    JOIN traindays t
    ON f.FL_DATE == t.FL_DATE
    WHERE
      t.is_train_day AND
      f.dep_delay IS NOT NULL
    ORDER BY
      f.dep_delay DESC
    """
    flights = spark.sql(statement)

    # quantiles
    distthresh = flights.approxQuantile('distance', list(np.arange(0, 1.0, 0.2)), 0.02)
    distthresh[-1] = float('inf')
    delaythresh = range(10, 20)
    logging.info("Computed distance thresholds: {}".format(distthresh))

    # bayes in each bin
    df = pd.DataFrame(columns=['dist_thresh', 'delay_thresh', 'frac_ontime'])
    for m in range(0, len(distthresh) - 1):
        for n in range(0, len(delaythresh) - 1):
            bdf = flights[(flights['distance'] >= distthresh[m])
                          & (flights['distance'] < distthresh[m + 1])
                          & (flights['dep_delay'] >= delaythresh[n])
                          & (flights['dep_delay'] < delaythresh[n + 1])]
            ontime_frac = bdf.agg(F.sum('ontime')).collect()[0][0] / bdf.agg(F.count('ontime')).collect()[0][0]
            print(m, n, ontime_frac)
            df = df.append({
                'dist_thresh': distthresh[m],
                'delay_thresh': delaythresh[n],
                'frac_ontime': ontime_frac
            }, ignore_index=True)

    # lookup table
    df['score'] = abs(df['frac_ontime'] - 0.7)
    bayes = df.sort_values(['score']).groupby('dist_thresh').head(1).sort_values('dist_thresh')
    bayes.to_csv('gs://{}/flights/bayes.csv'.format(BUCKET), index=False)
    logging.info("Wrote lookup table: {}".format(bayes.head()))


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Create Bayes lookup table')
    parser.add_argument('--bucket', help='GCS bucket to read/write data', required=True)
    parser.add_argument('--debug', dest='debug', action='store_true', help='Specify if you want debug messages')

    args = parser.parse_args()
    if args.debug:
        logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.DEBUG)
    else:
        logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)

    run_bayes(args.bucket)
