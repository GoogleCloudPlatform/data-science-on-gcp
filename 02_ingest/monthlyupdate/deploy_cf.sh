#!/bin/bash

#URL=ingest_flights_$(openssl rand -base64 48 | tr -d /=+ | cut -c -32)
URL=ingest_flights_monthly
echo $URL

gcloud functions deploy $URL --entry-point ingest_flights --runtime python37 --trigger-http --timeout 540s --allow-unauthenticated

