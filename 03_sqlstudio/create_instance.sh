#!/bin/bash
gcloud sql instances create flights \
    --tier=db-n1-standard-1 --activation-policy=ALWAYS

echo "Please go to the GCP console and change the root password of the instance"
