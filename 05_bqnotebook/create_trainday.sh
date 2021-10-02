#!/bin/bash

if [ "$#" -ne 1 ]; then
    echo "Usage: ./ingest_from_crsbucket.sh  destination-bucket-name"
    exit
fi

BUCKET=$1

cat trainday.txt | bq query --nouse_legacy_sql

bq extract dsongcp.trainday gs://${BUCKET}/flights/trainday.csv