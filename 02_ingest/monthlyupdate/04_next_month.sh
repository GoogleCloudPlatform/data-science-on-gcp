#!/bin/bash

# same as deploy_cr.sh
NAME=ingest-flights-monthly

PROJECT_ID=$(gcloud config get-value project)
BUCKET=${PROJECT_ID}-cf-staging

URL=$(gcloud run services describe ingest-flights-monthly --format 'value(status.url)')
echo $URL

# next month
echo "Getting month that follows ... (removing 12 if needed, so there is something to get) "
gsutil rm -rf gs://$BUCKET/flights/raw/201512.csv.gz
gsutil ls gs://$BUCKET/flights/raw
echo {\"bucket\":\"${BUCKET}\"\} > /tmp/message
cat /tmp/message

curl -k -X POST $URL \
   -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
   -H "Content-Type:application/json" --data-binary @/tmp/message

echo "Done"
