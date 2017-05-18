#!/bin/bash

bq load -F , flights2016.eval gs://cloud-training-demos-ml/flights/chapter10/eval/*.csv evaldata.json
