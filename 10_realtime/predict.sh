#!/bin/bash

gsutil -m rm -rf gs://cloud-training-demos-ml/flights/chapter10/output
bq rm -f flights.predictions

cd chapter10

mvn compile exec:java \
 -Dexec.mainClass=com.google.cloud.training.flights.AddRealtimePrediction \
 -Dexec.args="--realtime --speedupFactor=60 --maxNumWorkers=10 --autoscalingAlgorithm=THROUGHPUT_BASED"

