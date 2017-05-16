#!/bin/bash

bq load -F , flights.trainFlights gs://cloud-training-demos-ml/flights/chapter8/output/trainFlights* mldataset.json
bq load -F , flights.testFlights gs://cloud-training-demos-ml/flights/chapter8/output/testFlights* mldataset.json
