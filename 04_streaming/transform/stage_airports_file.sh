#!/bin/bash

if test "$#" -ne 1; then
   echo "Usage: ./bqsample.sh bucket-name"
   echo "   eg: ./bqsample.sh cloud-training-demos-ml"
   exit
fi

BUCKET=$1
PROJECT=$(gcloud config get-value project)

gsutil cp airports.csv.gz gs://${BUCKET}/flights/airports/airports.csv.gz

bq --project_id=$PROJECT load \
   --autodetect --replace --source_format=CSV \
   dsongcp.airports gs://${BUCKET}/flights/airports/airports.csv.gz