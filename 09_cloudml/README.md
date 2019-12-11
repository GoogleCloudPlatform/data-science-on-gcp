# Machine Learning Classifier using TensorFlow

### Catch up from previous chapters if necessary
If you didn't go through Chapters 2-8, the simplest way to catch up is to copy data from my bucket:
* Go to the 02_ingest folder of the repo, run the program ./ingest_from_crsbucket.sh and specify your bucket name.
* Go to the 04_streaming folder of the repo, run the program ./ingest_from_crsbucket.sh and specify your bucket name.
* Create a dataset named "flights" in BigQuery by typing:
	```
	bq mk flights
	```
* Go to the 05_bqnotebook folder of the repo, run the script to load data into BigQuery:
	```
	bash load_into_bq.sh <BUCKET-NAME>
	```
* In BigQuery, run this query and save the results as a table named trainday
	```
	  #standardsql
	SELECT
	  FL_DATE,
	  IF(MOD(ABS(FARM_FINGERPRINT(CAST(FL_DATE AS STRING))), 100) < 70, 'True', 'False') AS is_train_day
	FROM (
	  SELECT
	    DISTINCT(FL_DATE) AS FL_DATE
	  FROM
	    `flights.tzcorr`)
	ORDER BY
	  FL_DATE
	```
* Go to the 08_dataflow folder of the repo, run the program ./ingest_from_crsbucket.sh and specify your bucket name.


### This Chapter
You can do it two ways: from a notebook or from CloudShell.

#### Option 1: From a notebook
* Start Cloud AI Platform Notebook instance
* git clone this repository
* Run the cells in flights_caip.ipynb

#### Option 2: From CloudShell or in your local development machine
* Create a Docker image
  ```
  cd 09_cloudml/flights
  bash push_docker.sh
  ```
* Submit training job to Cloud AI Platform to train on the full dataset:
    ```
    ././train_model.sh  bucket-name linear 100000
    ```
  This will take about 1.5 hours.
* Deploy the trained model to Cloud AI Platform:
    ```
    ./deploy_model.sh bucket-name
    ```
  This will take 3-5 minutes.
* Get the model to predict:
    ```
    ./call_predict.py --project=$(gcloud config get-value core/project)
    ```
* Get the model to predict, but also provide a reason:
    ```
    ./call_predict_reason.py --project=$(gcloud config get-value core/project)
    ```
