# 6. Bayes Classifier on Cloud Dataproc

To repeat the steps in this chapter, follow these steps.

### Catch up from previous chapters if necessary
If you didn't go through Chapters 2-5, the simplest way to catch up is to copy data from my bucket:
* Go to the 02_ingest folder of the repo, run the program ./ingest_from_crsbucket.sh and specify your bucket name.
* Go to the 04_streaming folder of the repo, run the program ./ingest_from_crsbucket.sh and specify your bucket name.
* Create a dataset named "flights" in BigQuery by typing:
	```
	bq mk flights
	```
* Go to the 05_bqdatalab folder of the repo, run the script to load data into BigQuery:
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

### Create Dataproc cluster
In CloudShell:
* Clone the repository if you haven't already done so:
    ```
    git clone https://github.com/GoogleCloudPlatform/data-science-on-gcp
    ```
* Change to the directory for this chapter:
    ```
    cd data-science-on-gcp/06_dataproc
    ```
* Create the Dataproc cluster to run jobs on, specifying the name of your bucket and a 
  zone in the region that the bucket is in. (You created this bucket in Chapter 2)
   ```
    ./create_cluster.sh <BUCKET-NAME>  <COMPUTE-ZONE>
    ```
*Note:* Make sure that the compute zone is in the same region as the bucket, otherwise you will incur network egress charges.

### Interactive development
* Navigate to the Dataproc section of the GCP web console and click on "Web Interfaces".

* Click on JupyterLab

* In JupyterLab, navigate to /LocalDisks/home/dataproc/data-science-on-gcp

* Open 06_dataproc/quantization.ipynb. Click Run | Clear All Outputs. Then run the cells one by one.
 
* [optional] make the changes suggested in the notebook to run on the full dataset.  Note that you might have to
  reduce numbers to fit into your quota.
  
### Delete the cluster
* Delete the cluster either from the GCP web console or by typing in CloudShell, ```./delete_cluster.sh <YOUR REGION>```

### Serverless workflow
* Run ./submit_workflow.sh
 
