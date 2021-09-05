#!/bin/bash

# same as deploy_cf.sh
NAME=ingest-flights-monthly

PROJECT_ID=$(gcloud config get-value project)
BUCKET=${PROJECT_ID}-cf-staging

URL=$(gcloud run services describe ingest-flights-monthly --format 'value(status.url)')
echo $URL

echo {\"year\":\"2015\"\,\"month\":\"02\"\,\"bucket\":\"${BUCKET}\"\} > /tmp/message
cat /tmp/message

curl -k -X POST $URL \
   -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
   -H "Content-Type:application/json" --data-binary @/tmp/message

