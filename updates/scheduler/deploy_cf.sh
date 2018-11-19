#!/bin/bash

gcloud functions deploy ingest_flights --runtime python37 --trigger-http --timeout 480s
