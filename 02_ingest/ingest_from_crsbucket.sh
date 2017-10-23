#!/bin/bash

if [ "$#" -ne 1 ]; then
    echo "Usage: ./ingest_2015_from_crsbucket.sh  destination-bucket-name"
    exit
fi

BUCKET=$1

gsutil -m cp gs://data-science-on-gcp/flights/raw/* gs://$BUCKET/flights/raw

