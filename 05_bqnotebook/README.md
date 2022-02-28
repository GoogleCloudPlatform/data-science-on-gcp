# 5. Interactive data exploration

### Catch up from previous chapters if necessary
If you didn't go through Chapters 2-4, the simplest way to catch up is to copy data from my bucket:
* Go to the Storage section of the GCP web console and create a new bucket
* Open CloudShell and git clone this repo:
    ```
    git clone https://github.com/GoogleCloudPlatform/data-science-on-gcp
    ```
* Then, run:
    ```
    cd data-science-on-gcp/02_ingest
    ./ingest_from_crsbucket bucketname
    ./bqload.sh  (csv-bucket-name) YEAR 
    ```
* Run:
    ```
    cd ../03_sqlstudio
    ./create_views.sh
    ```
* Run:
    ```
    cd ../04_streaming
    ./ingest_from_crsbucket.sh
    ```

## Try out queries
* In BigQuery, query the time corrected files created in Chapter 4:
    ```
    SELECT
       ORIGIN,
       AVG(DEP_DELAY) AS dep_delay,
       AVG(ARR_DELAY) AS arr_delay,
       COUNT(ARR_DELAY) AS num_flights
     FROM
       dsongcp.flights_tzcorr
     GROUP BY
       ORIGIN
    ```
* Try out the other queries in queries.txt in this directory.

* Navigate to the Vertex AI Workbench part of the GCP console.

* Start a new managed notebook. Then, copy and paste cells from <a href="exploration.ipynb">exploration.ipynb</a> and click Run to execute the code.

* Create the trainday table BigQuery table and CSV file as you will need it later
    ```
    ./create_trainday.sh
    ```
