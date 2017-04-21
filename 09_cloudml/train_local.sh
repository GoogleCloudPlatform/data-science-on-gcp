#!/bin/bash

OUTPUT_DIR=./trained_model
DATA_DIR=~/data/flights

rm -rf $OUTPUT_DIR
export PYTHONPATH=${PYTHONPATH}:${PWD}/flights
python -m trainer.task \
   --output_dir=$OUTPUT_DIR \
   --job-dir=./tmp \
  --traindata $DATA_DIR/train* --evaldata $DATA_DIR/test* --learning_rate=0.001
