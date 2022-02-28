#!/bin/bash

if [ "$#" -ne 2 ]; then
    echo "Usage: ./create_cluster.sh  bucket-name  region"
    exit
fi

PROJECT=$(gcloud config get-value project)
BUCKET=$1
REGION=$2

# create cluster
gcloud dataproc clusters create ch7cluster \
  --enable-component-gateway \
  --region ${REGION} --zone ${REGION}-a \
  --master-machine-type n1-standard-4 \
  --master-boot-disk-size 500 \
  --num-workers 30 --num-secondary-workers 20 \
  --worker-machine-type n1-standard-8 \
  --worker-boot-disk-size 500 \
  --project $PROJECT \
  --scopes https://www.googleapis.com/auth/cloud-platform

gcloud dataproc autoscaling-policies import experiment-policy \
  --source=autoscale.yaml --region=$REGION

gcloud dataproc clusters update ch7cluster \
  --autoscaling-policy=experiment-policy --region=$REGION
