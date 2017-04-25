#!/bin/bash

BUCKET="cloud-training-demos-ml"
MODEL_NAME="flights"
MODEL_VERSION="v1"
REGION="us-central1"

# result of training
MODEL_LOCATION=$(gsutil ls gs://${BUCKET}/flights/chapter9/output/export/Servo/ | tail -1)
echo "Deleting and deploying $MODEL_NAME $MODEL_VERSION from $MODEL_LOCATION ... this will take a few minutes"

gcloud ml-engine versions delete ${MODEL_VERSION} --model ${MODEL_NAME}
#gcloud ml-engine models delete ${MODEL_NAME}
#gcloud ml-engine models create ${MODEL_NAME} --regions $REGION
gcloud ml-engine versions create ${MODEL_VERSION} --model ${MODEL_NAME} --origin ${MODEL_LOCATION}
