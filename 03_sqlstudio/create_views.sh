#!/bin/bash

PROJECT=$(gcloud config get-value project)
cat create_views.sql | bq --project_id $PROJECT query --nouse_legacy_sql
