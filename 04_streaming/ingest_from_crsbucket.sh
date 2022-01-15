#!/bin/bash

if [ "$#" -ne 1 ]; then
    echo "Usage: ./ingest_from_crsbucket.sh  destination-bucket-name"
    exit
fi

BUCKET=$1
FROM=gs://data-science-on-gcp/edition2/flights/tzcorr
TO=gs://$BUCKET/flights/tzcorr

#sharded files
CMD="gsutil -m cp "
for SHARD in `seq -w 0 26`; do
  CMD="$CMD ${FROM}/all_flights-000${SHARD}-of-00026"
done
CMD="$CMD $TO"
echo $CMD
$CMD

# load tzcorr into BigQuery
PROJECT=$(gcloud config get-value project)
bq --project_id $PROJECT \
  load --source_format=NEWLINE_DELIMITED_JSON --autodetect ${PROJECT}:dsongcp.flights_tzcorr \
  ${TO}/all_flights-*

cd transform

# airports.csv
./stage_airports_file.sh ${BUCKET}
