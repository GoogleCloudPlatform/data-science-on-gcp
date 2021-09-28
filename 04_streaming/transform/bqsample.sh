#!/bin/bash

if test "$#" -ne 1; then
   echo "Usage: ./bqsample.sh bucket-name"
   echo "   eg: ./bqsample.sh cloud-training-demos-ml"
   exit
fi

BUCKET=$1
PROJECT=$(gcloud config get-value project)

bq --project_id=$PROJECT query --destination_table dsongcp.flights_sample --replace --nouse_legacy_sql \
   'SELECT * FROM dsongcp.flights WHERE RAND() < 0.001'

bq --project_id=$PROJECT extract --destination_format=NEWLINE_DELIMITED_JSON \
   dsongcp.flights_sample  gs://${BUCKET}/flights/ch4/flights_sample.json

gsutil cp gs://${BUCKET}/flights/ch4/flights_sample.json .
