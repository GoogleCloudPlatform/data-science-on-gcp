#!/bin/bash

if [ "$#" -ne 1 ]; then
    echo "Usage: ./ingest.sh  destination-bucket-name"
    exit
fi

export BUCKET=$1

# get zip files from BTS, extract csv files
for YEAR in `seq 2015 2015`; do
   for MONTH in `seq 1 12`; do
      bash download.sh $YEAR $MONTH
      # upload the raw CSV files to our GCS bucket
      bash upload.sh $BUCKET
      rm *.csv
   done
   # load the CSV files into BigQuery as string columns
   bash bqload.sh $BUCKET $YEAR
done


# verify that things worked
bq query --nouse_legacy_sql \
  'SELECT DISTINCT year, month FROM dsongcp.flights_raw ORDER BY year ASC, CAST(month AS INTEGER) ASC'

bq query --nouse_legacy_sql \
  'SELECT year, month, COUNT(*) AS num_flights FROM dsongcp.flights_raw GROUP BY year, month ORDER BY year ASC, CAST(month AS INTEGER) ASC'
