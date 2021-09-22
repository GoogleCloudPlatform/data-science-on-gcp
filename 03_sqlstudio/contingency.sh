#!/bin/bash

PROJECT=$(gcloud config get-value project)
cat contingency4.sql \
   | bq --project_id $PROJECT query --nouse_legacy_sql
