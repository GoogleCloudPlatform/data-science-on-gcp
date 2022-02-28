#!/bin/bash

if [ "$#" -ne 3 ]; then
    echo "Usage: ./submit_spark_to_cluster.sh  bucket-name  region  pyspark-file"
    exit
fi

BUCKET=$1
REGION=$2
PYSPARK=$3

OUTDIR=gs://$BUCKET/flights/sparkmloutput

gsutil -m rm -r $OUTDIR

# submit to existing cluster
gsutil cp $PYSPARK $OUTDIR/$PYSPARK
gcloud dataproc jobs submit pyspark \
   --cluster ch7cluster --region $REGION \
   $OUTDIR/$PYSPARK \
   -- \
   --bucket $BUCKET
