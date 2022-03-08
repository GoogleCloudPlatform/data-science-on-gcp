# Machine Learning on Streaming Pipelines

### Catch up from previous chapters if necessary
If you didn't go through Chapters 2-9, the simplest way to catch up is to copy data from my bucket:

#### Catch up from Chapters 2-9
* Open CloudShell and git clone this repo:
    ```
    git clone https://github.com/GoogleCloudPlatform/data-science-on-gcp
    ```
* Go to the 02_ingest folder of the repo, run the program ./ingest_from_crsbucket.sh and specify your bucket name.
* Go to the 04_streaming folder of the repo, run the program ./ingest_from_crsbucket.sh and specify your bucket name.
* Go to the 05_bqnotebook folder of the repo, run the program ./create_trainday.sh and specify your bucket name.
* Go to the 10_mlops folder of the repo, run the program ./ingest_from_crsbucket.sh and specify your bucket name.

#### From CloudShell
* Install the Python libraries you'll need
    ```
    pip3 install google-cloud-aiplatform cloudml-hypertune pyfarmhash
    ```
* [Optional] Create a small, local sample of BigQuery datasets for local experimentation:
    ```
    bash create_sample_input.sh
    ```
* [Optional] Run a local pipeline to create a training dataset:
    ```
    python3 create_traindata.py --input local
    ```
   Verify the results:
   ```
   cat /tmp/all_data*
   ```
* Run a Dataflow pipeline to create the full training dataset:
  ```
    python3 create_traindata.py --input bigquery --project <PROJECT> --bucket <BUCKET> --region <REGION>
  ```
  Note if you get an error similar to:
  ```
  AttributeError: Can't get attribute '_create_code' on <module 'dill._dill' from '/usr/local/lib/python3.7/site-packages/dill/_dill.py'>
  ```
  it is because the global version of your modules are ahead/behind of what Apache Beam on the server requires. Make sure to submit Apache Beam code to Dataflow from a pristine virtual environment that has only the modules you need:
  ```
  python -m venv ~/beamenv
  source ~/beamenv/bin/activate
  pip install apache-beam[gcp] google-cloud-aiplatform cloudml-hypertune pyfarmhash pyparsing==2.4.2
  python3 create_traindata.py ...
  ```
  Note that beamenv is only for submitting to Dataflow. Run train_on_vertexai.py and other code directly in the terminal.
* Run script that copies over the Ch10 model.py and train_on_vertexai.py files and makes the necessary changes:
  ```
  python3 change_ch10_files.py
  ```
* [Optional] Train an AutoML model on the enriched dataset:
  ```
  python3 train_on_vertexai.py --automl --project <PROJECT> --bucket <BUCKET> --region <REGION>
  ```
  Verify performance by running the following BigQuery query:
  ```
  SELECT  
  SQRT(SUM(
      (CAST(ontime AS FLOAT64) - predicted_ontime.scores[OFFSET(0)])*
      (CAST(ontime AS FLOAT64) - predicted_ontime.scores[OFFSET(0)])
      )/COUNT(*))
  FROM dsongcp.ch11_automl_evaluated
  ```
* Train custom ML model on the enriched dataset:
  ```
  python3 train_on_vertexai.py --project <PROJECT> --bucket <BUCKET> --region <REGION>
  ```
  Look at the logs of the log to determine the final RMSE.
* Run a local pipeline to invoke predictions:
    ```
    python3 make_predictions.py --input local
    ```
   Verify the results:
   ```
   cat /tmp/predictions*
   ```
* [Optional] Run a pipeline on full BigQuery dataset to invoke predictions:
    ```
    python3 make_predictions.py --input bigquery --project <PROJECT> --bucket <BUCKET> --region <REGION>
    ```
   Verify the results
   ```
   gsutil cat gs://BUCKET/flights/ch11/predictions* | head -5
   ```
* [Optional] Simulate real-time pipeline and check to see if predictions are being made

  
   In one terminal, type:
    ```
  cd ../04_streaming/simulate
  python3 ./simulate.py --startTime '2015-05-01 00:00:00 UTC' \
           --endTime '2015-05-04 00:00:00 UTC' --speedFactor=30 --project <PROJECT>
    ```
   
  In another terminal type:
    ```
    python3 make_predictions.py --input pubsub \
           --project <PROJECT> --bucket <BUCKET> --region <REGION>
    ```
  
  Ensure that the pipeline starts, check that output elements are starting to be written out, do:
   ```
   gsutil ls gs://BUCKET/flights/ch11/predictions*
   ```
   Make sure to go to the GCP Console and stop the Dataflow pipeline.

  
* Simulate real-time pipeline and try out different jagger etc.

  In one terminal, type:
    ```
  cd ../04_streaming/simulate
  python3 ./simulate.py --startTime '2015-02-01 00:00:00 UTC' \
           --endTime '2015-02-03 00:00:00 UTC' --speedFactor=30 --project <PROJECT>
    ```
   
  In another terminal type:
    ```
    python3 make_predictions.py --input pubsub --output bigquery \
           --project <PROJECT> --bucket <BUCKET> --region <REGION>
    ```
  
  Ensure that the pipeline starts, look at BigQuery:
   ```
   SELECT * FROM dsongcp.streaming_preds ORDER BY event_time DESC LIMIT 10
   ```
   When done, make sure to go to the GCP Console and stop the Dataflow pipeline.
   
   Note: If you are going to try it a second time around, delete the BigQuery sink, or simulate with a different time range
   ```
   bq rm -f dsongcp.streaming_preds
   ```
  
