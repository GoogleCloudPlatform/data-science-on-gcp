#!/bin/bash

if [ "$#" -ne 2 ]; then
   echo "Usage:   ./submit_workflow.sh bucket-name region"
   exit
fi

TEMPLATE=ch6eph
MACHINE_TYPE=n1-standard-4
CLUSTER=ch6eph
BUCKET=$1
REGION=$2

#gsutil cp bayes_on_spark.py gs://$BUCKET/

gcloud dataproc --quiet workflow-templates delete $TEMPLATE
gcloud dataproc --quiet workflow-templates create $TEMPLATE

exit

# the things we need pip-installed on the cluster
STARTUP_SCRIPT=gs://${BUCKET}/${CLUSTER}/startup_script.sh
echo "pip install --upgrade --quiet google-api-python-client" > /tmp/startup_script.sh
gsutil cp /tmp/startup_script.sh $STARTUP_SCRIPT

# create new cluster for job
gcloud dataproc workflow-templates set-managed-cluster $TEMPLATE \
    --region ${REGION} \
    --master-machine-type $MACHINE_TYPE \
    --worker-machine-type $MACHINE_TYPE \
    --initialization-actions $STARTUP_SCRIPT \
    --num-secondary-workers=3 --num-workers 2 \
    --image-version 2.0 \
    --cluster-name $CLUSTER

# steps in job
gcloud dataproc workflow-templates add-job \
  pyspark gs://$BUCKET/bayes_on_spark.py \
  --step-id create-report \
  --workflow-template $TEMPLATE \
  -- --bucket=$BUCKET
  
  
# submit workflow template
gcloud dataproc workflow-templates instantiate $TEMPLATE
