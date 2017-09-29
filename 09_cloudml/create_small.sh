#!/bin/bash

if [ "$#" -ne 1 ]; then
    echo "Usage: ./create_small.sh  bucket-name"
    exit
fi

BUCKET=$1

rm -rf ~/data/flights
mkdir -p ~/data/flights

for STEP in train test; do
  gsutil cp gs://${BUCKET}/flights/chapter8/output/${STEP}Flights-00001*.csv full.csv
  head -10003 full.csv > ~/data/flights/${STEP}.csv
  rm full.csv
done
