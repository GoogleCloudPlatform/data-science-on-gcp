#!/bin/sh
bq mk --external_table_definition=./airport_schema.json@CSV=gs://data-science-on-gcp/edition2/raw/airports.csv dsongcp.airports_gcs
