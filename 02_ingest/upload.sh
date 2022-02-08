#!/bin/bash
export BUCKET=${BUCKET:=uncc6155-spring2022-sharanya}
echo "Uploading to bucket $BUCKET..."
gsutil -m cp *.csv gs://$BUCKET/flights/raw
#gsutil -m acl ch -R -g google.com:R gs://$BUCKET/flights/raw
