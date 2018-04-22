#!/bin/bash

TF_VERSION="1.4"   # CHANGE as needed

if [ "$#" -ne 2 ]; then
    echo "Usage: ./deploy_model.sh bucket-name bucket-region"
    exit
fi

BUCKET=$1
REGION=$2

MODEL_NAME="flights"
MODEL_VERSION="v1"

# result of training
MODEL_LOCATION=$(gsutil ls gs://${BUCKET}/flights/chapter9/output/export/Servo/ | tail -1)

echo "Deleting and deploying $MODEL_NAME $MODEL_VERSION from $MODEL_LOCATION ... this will take a few minutes ... the first two calls to delete version/model will throw ERROR if this is the first time you are deploying the model (you can ignore those)"

gcloud ml-engine versions delete ${MODEL_VERSION} --model ${MODEL_NAME}
gcloud ml-engine models delete ${MODEL_NAME}
gcloud ml-engine models create ${MODEL_NAME} --regions $REGION
gcloud ml-engine versions create ${MODEL_VERSION} --model ${MODEL_NAME} --origin ${MODEL_LOCATION} --runtime-version=${TF_VERSION}
