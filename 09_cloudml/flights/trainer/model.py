from tensorflow.contrib.learn.python.learn.utils import saved_model_export_utils
import tensorflow.contrib.learn as tflearn
import tensorflow.contrib.layers as tflayers

import tensorflow as tf

CSV_COLUMNS  = ('ontime,dep_delay,taxiout,distance,avg_dep_delay,avg_arr_delay' + \
                ',carrier,dep_lat,dep_lon,arr_lat,arr_lon,origin,dest').split(',')
LABEL_COLUMN = 'ontime'
DEFAULTS     = [[0.0],[0.0],[0.0],[0.0],[0.0],[0.0],\
                ['na'],[0.0],[0.0],[0.0],[0.0],['na'],['na']]

def read_dataset(filename, mode=tf.contrib.learn.ModeKeys.EVAL, batch_size=512, num_training_epochs=10):

  # the actual input function passed to TensorFlow
  def _input_fn():
    num_epochs = num_training_epochs if mode == tf.contrib.learn.ModeKeys.TRAIN else 1

    # could be a path to one file or a file pattern.
    input_file_names = tf.train.match_filenames_once(filename)
    filename_queue = tf.train.string_input_producer(
        input_file_names, num_epochs=num_epochs, shuffle=True)
 
    # read CSV
    reader = tf.TextLineReader()
    _, value = reader.read_up_to(filename_queue, num_records=batch_size)
    value_column = tf.expand_dims(value, -1)
    columns = tf.decode_csv(value_column, record_defaults=DEFAULTS)
    features = dict(zip(CSV_COLUMNS, columns))
    label = features.pop(LABEL_COLUMN)
    return features, label
  
  return _input_fn

def get_features():
    real = {
      colname : tflayers.real_valued_column(colname) \
          for colname in \
            ('dep_delay,taxiout,distance,avg_dep_delay,avg_arr_delay' + 
             ',dep_lat,dep_lon,arr_lat,arr_lon').split(',')
    }
    sparse = {
      'carrier': tflayers.sparse_column_with_keys('carrier',
                  keys='AS,VX,F9,UA,US,WN,HA,EV,MQ,DL,OO,B6,NK,AA'.split(',')),
      'origin' : tflayers.sparse_column_with_hash_bucket('origin', hash_bucket_size=1000),
      'dest'   : tflayers.sparse_column_with_hash_bucket('dest', hash_bucket_size=1000)
    }
    return real, sparse

def wide_and_deep_model(output_dir):
    deep, wide = get_features()
    return \
        tflearn.DNNLinearCombinedClassifier(model_dir=output_dir,
                                           linear_feature_columns=wide,
                                           dnn_feature_columns=deep,
                                           dnn_hidden_units=[64, 32])
   
def linear_model(output_dir):
    real, sparse = get_features()
    all = {}
    all.update(real)
    all.update(sparse)
    return tflearn.LinearClassifier(model_dir=output_dir,
                                    feature_columns=all.values())


def serving_input_fn():
    real, sparse = get_features()
    
    feature_placeholders = {
      key : tf.placeholder(tf.float32, [None]) \
        for key in real.keys()
    }
    feature_placeholders.update( {
      key : tf.placeholder(tf.string, [None]) \
        for key in sparse.keys()
    } )

    features = {
      key: tf.expand_dims(tensor, -1)
      for key, tensor in feature_placeholders.items()
    }
    return tflearn.utils.input_fn_utils.InputFnOps(
      features,
      None,
      feature_placeholders)


def make_experiment_fn(traindata, evaldata, **args):
  def _experiment_fn(output_dir):
    return tflearn.Experiment(
        linear_model(output_dir),
        train_input_fn=read_dataset(traindata, mode=tf.contrib.learn.ModeKeys.TRAIN),
        eval_input_fn=read_dataset(evaldata),
        export_strategies=[saved_model_export_utils.make_export_strategy(
            serving_input_fn,
            default_output_alternative_key=None,
            exports_to_keep=1
        )],
        **args
    )
  return _experiment_fn
