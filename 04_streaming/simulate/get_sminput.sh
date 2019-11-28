#!/bin/bash
gsutil cat gs://cloud-training-demos/flights/raw/201501.csv | head -1000 > 201501_part.csv
