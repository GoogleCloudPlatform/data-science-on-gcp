#!/bin/bash

if [ "$#" -ne 3 ]; then
    echo "Usage: ./submit_spark.sh  bucket-name  region  pyspark-file"
    exit
fi

BUCKET=$1
REGION=$2
PYSPARK=$3

OUTDIR=gs://$BUCKET/flights/sparkmloutput

gsutil -m rm -r $OUTDIR

# submit to existing cluster
#gcloud dataproc jobs submit pyspark \
#   --cluster ch6cluster --region $REGION \
#   logistic.py \
#   -- \
#   --bucket $BUCKET --debug

# Serverless Spark
gsutil cp $PYSPARK $OUTDIR/$PYSPARK
gcloud beta dataproc batches submit pyspark \
   --project=$(gcloud config get-value project) \
   --region=$REGION \
   $OUTDIR/$PYSPARK \
   -- \
   --bucket ${BUCKET} --debug