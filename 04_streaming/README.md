# 4. Streaming data: publication and ingest

### Catch up until Chapter 3 if necessary
* Go to the Storage section of the GCP web console and create a new bucket
* Open CloudShell and git clone this repo:
    ```
    git clone https://github.com/GoogleCloudPlatform/data-science-on-gcp
    ```
* Then, run:
```
cd data-science-on-gcp/02_ingest
./ingest_from_crsbucket bucketname
```
* Run:
```
cd ../03_sqlstudio
./create_views.sh
```

### Batch processing transformation in DataFlow
* Setup:
    ```
	cd transform; ./install_packages.sh
    ```
* Parsing airports data:
	```
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
* Read/write to Cloud:
	```
    ./stage_airports_file.sh BUCKETNAME
	./df06.py --project PROJECT --bucket BUCKETNAME
	``` 
    Look for new tables in BigQuery (flights_simevents)
* Run on Cloud:
	```
	./df07.py --project PROJECT --bucket BUCKETNAME --region us-central1
	``` 
* Go to the GCP web console and wait for the Dataflow ch04timecorr job to finish. It might take between 30 minutes and 2+ hours depending on the quota associated with your project (you can change the quota by going to https://console.cloud.google.com/iam-admin/quotas).
* Then, navigate to the BigQuery console and type in:
	```
        SELECT
          ORIGIN,
          DEP_TIME,
          DEST,
          ARR_TIME,
          ARR_DELAY,
          EVENT_TIME,
          EVENT_TYPE
        FROM
          dsongcp.flights_simevents
        WHERE
          (DEP_DELAY > 15 and ORIGIN = 'SEA') or
          (ARR_DELAY > 15 and DEST = 'SEA')
        ORDER BY EVENT_TIME ASC
        LIMIT
          5

	```
### Simulate event stream
* In CloudShell, run
	```
    cd simulate
	python3 ./simulate.py --startTime '2015-05-01 00:00:00 UTC' --endTime '2015-05-04 00:00:00 UTC' --speedFactor=30 --project $DEVSHELL_PROJECT_ID
    ```
 
### Real-time Stream Processing
* In another CloudShell tab, run avg01.py:
	```
	cd realtime
	./avg01.py --project PROJECT --bucket BUCKETNAME --region us-central1
	```
* In about a minute, you can query events from the BigQuery console:
	```
	SELECT * FROM dsongcp.streaming_events
	ORDER BY EVENT_TIME DESC
    LIMIT 5
	```
* Stop avg01.py by hitting Ctrl+C
* Run avg02.py:
	```
	./avg02.py --project PROJECT --bucket BUCKETNAME --region us-central1
	```
* In about 5 min, you can query from the BigQuery console:
	```
	SELECT * FROM dsongcp.streaming_delays
	ORDER BY END_TIME DESC
    LIMIT 5
	``` 
* Look at how often the data is coming in:
	```
    SELECT END_TIME, num_flights
    FROM dsongcp.streaming_delays
    ORDER BY END_TIME DESC
    LIMIT 5
	``` 
* It's likely that the pipeline will be stuck. You need to run this on Dataflow.
* Stop avg02.py by hitting Ctrl+C
* In BigQuery, truncate the table:
	```
	TRUNCATE TABLE dsongcp.streaming_delays
	``` 
* Run avg03.py:
	```
	./avg03.py --project PROJECT --bucket BUCKETNAME --region us-central1
	```
* Go to the GCP web console in the Dataflow section and monitor the job.
* Once the job starts writing to BigQuery, run this query and save this as a view:
	```
	SELECT * FROM dsongcp.streaming_delays
    WHERE AIRPORT = 'ATL'
    ORDER BY END_TIME DESC
	```
* Create a view of the latest arrival delay by airport:
	```
    CREATE OR REPLACE VIEW dsongcp.airport_delays AS
    WITH delays AS (
        SELECT d.*, a.LATITUDE, a.LONGITUDE
        FROM dsongcp.streaming_delays d
        JOIN dsongcp.airports a USING(AIRPORT) 
        WHERE a.AIRPORT_IS_LATEST = 1
    )
     
    SELECT 
        AIRPORT,
        CONCAT(LATITUDE, ',', LONGITUDE) AS LOCATION,
        ARRAY_AGG(
            STRUCT(AVG_ARR_DELAY, AVG_DEP_DELAY, NUM_FLIGHTS, END_TIME)
            ORDER BY END_TIME DESC LIMIT 1) AS a
    FROM delays
    GROUP BY AIRPORT, LONGITUDE, LATITUDE

	```   
* Follow the steps in the chapter to connect to Data Studio and create a GeoMap.
* Stop the simulation program in CloudShell.
* From the GCP web console, stop the Dataflow streaming pipeline.

