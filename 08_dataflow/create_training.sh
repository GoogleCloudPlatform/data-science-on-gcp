#!/bin/bash
OUTDIR=gs://cloud-training-demos-ml/flights/chapter8/output/


STAGE=depdelay
#STAGE=training
#STAGE=test

mvn compile exec:java \
 -Dexec.mainClass=com.google.cloud.training.flights.CreateDatasets \
      -Dexec.args="--fullDataset --stage=$STAGE --maxNumWorkers=10 \
                   --autoscalingAlgorithm=THROUGHPUT_BASED \
                   --stagingLocation=$OUTDIR \
                   --output=$OUTDIR"

