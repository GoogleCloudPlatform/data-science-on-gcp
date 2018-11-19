#!/bin/bash

REGION='us-central1'
PROJECT=$(gcloud config get-value project)
BUCKET=cloud-training-demos-ml

echo {\"year\":\"2015\"\,\"month\":\"03\"\,\"bucket\":\"${BUCKET}\"} > /tmp/message
cat /tmp/message

curl -X POST "https://${REGION}-${PROJECT}.cloudfunctions.net/ingest_flights" -H "Content-Type:application/json" --data-binary @/tmp/message

