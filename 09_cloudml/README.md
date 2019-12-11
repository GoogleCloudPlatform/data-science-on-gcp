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
In CloudShell, or in your local development machine:
* Create a small sample of the full dataset you created in Chapter 8 in ~/data:
    ```
    cd data-science-on-gcp/09_cloudml
    ./create_small.sh bucket-name
    ```
* If in CloudShell, enable Boost mode. Then, install tensorflow:
    ```
    pip install tensorflow
    ```
* Train locally:
    ```
    ./train_local.sh
    ```
* Submit training job to Cloud AI Platform to train on the full dataset:
    ```
    ./train_cloud.sh bucket-name region-name
    ```
  This will take about 1.5 hours.
* Deploy the trained model to Cloud ML Engine:
    ```
    ./deploy_model.sh bucket-name region-name
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
