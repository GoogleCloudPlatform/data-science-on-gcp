#!/bin/bash

if [ "$#" -ne 2 ]; then
    echo "Usage:        ./deploy_model.sh bucket best_model"
    echo "   eg:        ./deploy_model.sh cloud-training-demos-ml 15/"
    echo "   eg:        ./deploy_model.sh cloud-training-demos-ml ''"
    exit
fi

BUCKET=$1
PROJECT=$(gcloud config get-value project)

BEST_MODEL=$2   # use an empty string if you didn't do hyperparam tuning

MODEL_NAME=flights
VERSION_NAME=tf2
REGION=us-central1
EXPORT_PATH=$(gsutil ls gs://$BUCKET/flights/trained_model/${BEST_MODEL}export | tail -1)
echo $EXPORT_PATH

if [[ $(gcloud ai-platform models list --format='value(name)' | grep $MODEL_NAME) ]]; then
    echo "$MODEL_NAME already exists"
else
    # create model
    echo "Creating $MODEL_NAME"
    gcloud ai-platform models create --regions=$REGION $MODEL_NAME
fi

if [[ $(gcloud ai-platform versions list --model $MODEL_NAME --format='value(name)' | grep $VERSION_NAME) ]]; then
    echo "Deleting already existing $MODEL_NAME:$VERSION_NAME ... "
    gcloud ai-platform versions delete --quiet --model=$MODEL_NAME $VERSION_NAME
    echo "Please run this cell again if you don't see a Creating message ... "
    sleep 10
fi

# create model
echo "Creating $MODEL_NAME:$VERSION_NAME"
gcloud ai-platform versions create --model=$MODEL_NAME $VERSION_NAME --async \
       --framework=tensorflow --python-version=3.5 --runtime-version=1.14 \
       --origin=$EXPORT_PATH --staging-bucket=gs://$BUCKET