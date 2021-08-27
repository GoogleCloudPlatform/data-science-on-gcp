#!/bin/bash

export YEAR=${YEAR:=2015}

# get zip files from BTS, extract csv files
for MONTH in `seq 1 12`; do
   bash download.sh $YEAR $MONTH
done

# upload the raw CSV files to our GCS bucket
bash upload.sh

# load the CSV files into BigQuery as string columns
bash bqload.sh

# verify that things worked
bq query --nouse_legacy_sql \
  'SELECT DISTINCT month FROM dsongcp.flights_raw ORDER BY CAST(month AS INTEGER) ASC'
