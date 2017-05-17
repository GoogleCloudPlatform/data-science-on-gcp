#!/bin/bash
export YEAR=2016
bash download.sh
bash zip_to_csv.sh
bash quotes_comma.sh
gsutil -m cp *.csv gs://$BUCKET/flights2016/raw
