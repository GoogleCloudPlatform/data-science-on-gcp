#!/bin/bash

if [ "$#" -ne 1 ]; then
    echo "Usage: ./upload.sh  destination-bucket-name"
    exit
fi

BUCKET=$1

echo "Uploading to bucket $BUCKET..."
gsutil -m cp *.csv gs://$BUCKET/flights/raw/
#gsutil -m acl ch -R -g allUsers:R gs://$BUCKET/flights/raw
#gsutil -m acl ch -R -g google.com:R gs://$BUCKET/flights/raw
