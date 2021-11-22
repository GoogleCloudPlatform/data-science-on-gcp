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
    python3 create_traindata.py --input bigquery --project <PROJECT> --bucket <BUCKET> --region us-central1
  ```
* [Optional] Run a local pipeline to invoke predictions:
    ```
    python3 make_predictions.py --input local
    ```
   Verify the results:
   ```
   cat /tmp/predictions*
   ```

