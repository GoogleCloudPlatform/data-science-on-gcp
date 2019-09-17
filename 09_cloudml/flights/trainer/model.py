import logging

import tensorflow as tf
import numpy as np

CSV_COLUMNS  = ('ontime,dep_delay,taxiout,distance,avg_dep_delay,avg_arr_delay' + \
                ',carrier,dep_lat,dep_lon,arr_lat,arr_lon,origin,dest').split(',')
LABEL_COLUMN = 'ontime'
DEFAULTS     = [[0.0],[0.0],[0.0],[0.0],[0.0],[0.0],\
                ['na'],[0.0],[0.0],[0.0],[0.0],['na'],['na']]

BUCKET = None
OUTPUT_DIR = None
NBUCKETS = None
NUM_EXAMPLES = None
TRAIN_BATCH_SIZE = None
TRAIN_DATA_PATTERN = None
EVAL_DATA_PATTERN = None

def setup(args):
    global BUCKET, OUTPUT_DIR, NBUCKETS, NUM_EXAMPLES
    global TRAIN_BATCH_SIZE, TRAIN_DATA_PATTERN, EVAL_DATA_PATTERN
    BUCKET = args['bucket']
    OUTPUT_DIR = args['output_dir']
    NBUCKETS = int(args['nbuckets'])
    NUM_EXAMPLES = int(args['num_examples'])
    TRAIN_BATCH_SIZE = int(args['train_batch_size'])

    # set up training and evaluation data patterns
    DATA_BUCKET = "gs://{}/flights/chapter8/output/".format(BUCKET)
    TRAIN_DATA_PATTERN = DATA_BUCKET + "train*"
    EVAL_DATA_PATTERN = DATA_BUCKET + "test*"
    logging.info('Training based on data in {}'.format(TRAIN_DATA_PATTERN))
    
def features_and_labels(features):
  label = features.pop('ontime') # this is what we will train for
  return features, label

def read_dataset(pattern, batch_size, mode=tf.estimator.ModeKeys.TRAIN, truncate=None):
  dataset = tf.data.experimental.make_csv_dataset(pattern, batch_size, CSV_COLUMNS, DEFAULTS)
  dataset = dataset.map(features_and_labels)
  if mode == tf.estimator.ModeKeys.TRAIN:
    dataset = dataset.repeat()
    dataset = dataset.shuffle(batch_size*10)
  dataset = dataset.prefetch(1)
  if truncate is not None:
    dataset = dataset.take(truncate)
  return dataset

def read_lines():
    if TRAIN_BATCH_SIZE > 5:
        print('This is meant for trying out the input pipeline. Please set train_batch_size to be < 5')
        return
    
    logging.info("Checking input pipeline batch_size={}".format(TRAIN_BATCH_SIZE))
    one_item = read_dataset(TRAIN_DATA_PATTERN, TRAIN_BATCH_SIZE, truncate=1)
    print(list(one_item)) # should print one batch of items
    
def find_average_label():
    logging.info("Finding average label in num_examples={}".format(NUM_EXAMPLES))
    features_and_labels = read_dataset(TRAIN_DATA_PATTERN, 1, truncate=NUM_EXAMPLES)
    labels = features_and_labels.map(lambda x, y : y)
    count, sum = labels.reduce((0.0,0.0), lambda state, y: (state[0]+1.0, state[1]+y))
    print(sum/count) # average of the whole lot
