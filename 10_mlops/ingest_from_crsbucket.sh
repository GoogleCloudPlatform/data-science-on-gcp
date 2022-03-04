#!/bin/bash

if [ "$#" -ne 1 ]; then
    echo "Usage: ./ingest_from_crsbucket.sh  destination-bucket-name"
    exit
fi

BUCKET_NAME=$1

for split in train eval all; do
   gsutil cp gs://data-science-on-gcp/edition2/ch9/data/${split}.csv gs://$BUCKET_NAME/ch9/data/${split}.csv
done
