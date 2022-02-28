#!/bin/bash

if [ "$#" -ne 2 ]; then
    echo "Usage: ./create_cluster.sh  bucket-name  region"
    exit
fi

PROJECT=$(gcloud config get-value project)
BUCKET=$1
REGION=$2
EMAIL=$3
INSTALL=gs://$BUCKET/flights/dataproc/install_on_cluster.sh

# upload install file
sed "s/CHANGE_TO_USER_NAME/dataproc/g" install_on_cluster.sh > /tmp/install_on_cluster.sh
gsutil cp /tmp/install_on_cluster.sh $INSTALL

# create cluster
gcloud dataproc clusters create ch6cluster \
  --enable-component-gateway \
  --region ${REGION} --zone ${REGION}-a \
  --master-machine-type n1-standard-4 \
  --master-boot-disk-size 500 --num-workers 2 \
  --worker-machine-type n1-standard-4 \
  --worker-boot-disk-size 500 \
  --optional-components JUPYTER --project $PROJECT \
  --initialization-actions=$INSTALL \
  --scopes https://www.googleapis.com/auth/cloud-platform

