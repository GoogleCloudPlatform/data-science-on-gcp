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
* Go to the 05_bqnotebook folder of the repo, run the script to load data into BigQuery:
	```
	bash create_trainday.sh <BUCKET-NAME>
	```

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
* Copy over the Ch9 model.py and train_on_vertexai.py files and make the necessary changes:
  ```
  python3 change_ch9_files.py
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
  FROM dsongcp.ch10_automl_evaluated
  ```
* Train custom ML model on the enriched dataset:
  ```
  python3 train_on_vertexai.py --project <PROJECT> --bucket <BUCKET> --region <REGION>
  ```
  Look at the logs of the log to determine the final RMSE.
* [Optional] Run a local pipeline to invoke predictions:
    ```
    python3 make_predictions.py --input local
    ```
   Verify the results:
   ```
   cat /tmp/predictions*
   ```

