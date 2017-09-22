To follow the steps in the book:
* Parsing airports data:
    ```
    cd 04_streaming/simulate
    ./install_packages.sh
    ./df01.py
    head extracted_airports-00000*
    rm extracted_airports-*
    ```
* Adding timezone information:
    ```
    ./df02.py
    head airports_with_tz-00000*
    rm airports_with_tz-*
    ```
* Converting times to UTC:
   ```
    ./df03.py
    head -3 all_flights-00000*
   ```
* Correcting dates:
    ```
    ./df04.py
    head -3 all_flights-00000*
    rm all_flights-*
    ```
* Create events:
     ```
    ./df05.py
    head -3 all_events-00000*
    rm all_events-*
    ```  
* Pipeline on GCP:
     * Go to the GCP web console, API & Services section and enable the Dataflow API.
     * In CloudShell, type:
       ```
       bq mk flights
       gsutil cp airports.csv.gz gs://<BUCKET-NAME>/flights/airports/airports.csv.gz
       ./df06.py -p $DEVSHELL_PROJECT_ID -b <BUCKETNAME> 
       ``` 
     * Go to the GCP web console and wait for the Dataflow ch04timecorr job to finish. It might take several  
     * Then, navigate to the BigQuery console and type in:
    ```
SELECT
  ORIGIN,
  DEP_TIME,
  DEP_DELAY,
  DEST,
  ARR_TIME,
  ARR_DELAY,
  NOTIFY_TIME
FROM
  flights.simevents
WHERE
  (DEP_DELAY > 15 and ORIGIN = 'SEA') or
  (ARR_DELAY > 15 and DEST = 'SEA')
ORDER BY NOTIFY_TIME ASC
LIMIT
  10
    ```
* Publishing event stream to Pub/Sub
  * Follow the OAuth2 workflow so that the python script can run code on your behalf:
```
gcloud auth application-default login
```
  * Run
```
python simulate.py --startTime '2015-05-01 00:00:00 UTC' --endTime '2015-05-04 00:00:00 UTC' --speedFactor=60

    ```
  * 
    