# Full Dataset

#### [Optional] Train on 2015-2018 and evaluate on 2019
Note that this will take many hours and require significant resources.
There is a reason why I have worked with only 1 year of data so far in the book.
* [5 min] Erase the current contents of your bucket and BigQuery dataset:
  ```
  gsutil -m rm -rf gs://BUCKET/*
  bq rm -r -f dsongcp
  ```
* [28h or 2 min] Create Training Dataset OR Copy it from my bucket
  * [28 hours] Create Training Dataset
    * [30 min] Ingest raw files:
      * cd 02_ingest
      * Edit the YEARS in 02_ingest/ingest.sh to process 2015 to 2019.
      * Run ./ingest.sh program
    * [2 min] Create views
      * cd ../03_sqlstudio
      * ./create_views.sh
    * [40 min] Do time correction
      * cd ../04_streaming/transform
      * ./stage_airports_file.sh $BUCKET
      * Increase number of workers in df07.py to 20 or the limit of your quota
      * python3 df07.py --project $PROJECT --bucket $BUCKET --region $REGION 
    * [26 hours] Create training dataset
      * cd ../11_realtime
      * Edit flightstxf/create_traindata.py changing the line
        ```
        'data_split': get_data_split(event['FL_DATE'])
        ```
        to
        ```
        'data_split': get_data_split_2019(event['FL_DATE'])
        ```
      * Change the worker type to m1-ultramem-40 and disksize to 500 GB in the run() method of create_traindata.py.
      * Create full training dataset
        ```
        python3 create_traindata.py --input bigquery --project $PROJECT --bucket $BUCKET --region $REGION
        ```
  * [2 min] Copy the full training data set from my bucket:
      ```
      gsutil cp \
         gs://data-science-on-gcp/edition2/ch12_fulldataset/all-00000-of-00001.csv \
         gs://$BUCKET/ch11/data/all-00000-of-00001.csv
      ```
 
* [5 hr] Train AutoML model so that we have evaluation statistics in BigQuery:
  ```
  cd 11_realtime
  python3 train_on_vertexai.py --automl --project $PROJECT --bucket $BUCKET --region $REGION
  ```
* Open the notebook evaluation.ipynb in Vertex Workbench and run the cells.
