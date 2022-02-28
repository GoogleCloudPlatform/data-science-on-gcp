#!/bin/bash

REGION=us-central1
ENDPOINT_NAME=flights

ENDPOINT_ID=$(gcloud ai endpoints list --region=$REGION \
              --format='value(ENDPOINT_ID)' --filter=display_name=${ENDPOINT_NAME} \
              --sort-by=creationTimeStamp | tail -1)
echo $ENDPOINT_ID
gcloud ai endpoints predict $ENDPOINT_ID --region=$REGION --json-request=example_input.json
