# 2. Ingesting data onto the Cloud

### Create a bucket
* Go to the Storage section of the GCP web console and create a new bucket

### Populate your bucket with the data you will need for the book

* Open CloudShell and git clone this repo:
    ```
    git clone https://github.com/GoogleCloudPlatform/data-science-on-gcp
    ```
* Go to the 02_ingest folder of the repo
* Edit ./ingest.sh to reflect the years you want to process (at minimum, you need 2015)
* Execute ./ingest.sh bucketname

### [Optional] Scheduling monthly downloads
* Go to the 02_ingest/monthlyupdate folder in the repo.
* Run the command `pip3 install google-cloud-storage google-cloud-bigquery`
* Run the command `gcloud auth application-default login`
* Try ingesting one month using the Python script: `./ingest_flights.py --debug --bucket your-bucket-name --year 2015 --month 02` 
* Set up a service account called svc-monthly-ingest by running `./01_setup_svc_acct.sh`
* Now, try running the ingest script as the service account:
  * Visit the Service Accounts section of the GCP Console: https://console.cloud.google.com/iam-admin/serviceaccounts
  * Select the newly created service account svc-monthly-ingest and click Manage Keys
  * Add key (Create a new JSON key) and download it to a file named tempkey.json
  * Run `gcloud auth activate-service-account --key-file tempkey.json`
  * Try ingesting one month `./ingest_flights.py --bucket $BUCKET --year 2015 --month 03 --debug`
  * Go back to running command as yourself using `gcloud auth login`
* Deploy to Cloud Run: `./02_deploy_cr.sh`
* Test that you can invoke the function using Cloud Run: `./03_call_cr.sh`
* Test that the functionality to get the next month works: `./04_next_month.sh`
* Set up a Cloud Scheduler job to invoke Cloud Run every month: `./05_setup_cron.sh`
* Visit the GCP Console for Cloud Run and Cloud Scheduler and delete the Cloud Run instance and the scheduled task—you won’t need them any further.
