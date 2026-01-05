#!/bin/bash

if [ "$#" -ne 1 ]; then
    echo "Usage: ./upload.sh  destination-bucket-name"
    exit
fi

BUCKET=$1

echo "Uploading to bucket $BUCKET..."
gcloud storage cp *.csv gs://$BUCKET/flights/raw/
# gcloud storage objects update "gs://$BUCKET/flights/raw/**" --add-acl-grant=entity=allUsers,role=READER
# gcloud storage objects update "gs://$BUCKET/flights/raw/**" --add-acl-grant=entity=domain-google.com,role=READER
