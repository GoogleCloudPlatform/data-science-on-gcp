#!/bin/bash

if [ "$#" -ne 2 ]; then
   echo "Usage:   ./submit_serverless.sh bucket-name region"
   exit
fi

BUCKET=$1
REGION=$2

# Note: The "default" network in the region needs to be enabled
# for private Google access
# https://cloud.google.com/vpc/docs/configure-private-google-access#config-pga

gsutil cp bayes_on_spark.py gs://$BUCKET/

gcloud beta dataproc batches submit pyspark \
   --project=$(gcloud config get-value project) \
   --region=$REGION \
   gs://${BUCKET}/bayes_on_spark.py \
   -- \
   --bucket ${BUCKET} # --debug
