# 3. Creating compelling dashboards

### Catch up to Chapter 2
If you have not already done so, load the raw data into a BigQuery dataset:
* Go to the Storage section of the GCP web console and create a new bucket
* Open CloudShell and git clone this repo:
    ```
    git clone https://github.com/GoogleCloudPlatform/data-science-on-gcp
    ```
* Then, run:
    ```
    cd data-science-on-gcp/02_ingest
    ./ingest.sh bucketname
    ```


### Optional: Load the data into PostgreSQL
* Navigate to https://console.cloud.google.com/sql
* Select Create Instance
* Choose PostgreSQL and then fill out the form as follows:
  * Call the instance flights
  * Generate a strong password by clicking GENERATE
  * Choose the default PostgreSQL version
  * Choose the region where your bucket of CSV data exists
  * Choose a single zone instance
  * Choose a standard machine type with 2 vCPU
  * Click Create Instance
*  Type (change bucket as necessary):
  ```
   gsutil cp create_table.sql \
    gs://cloud-training-demos-ml/flights/ch3/create_table.sql
  ```
* Create empty table using web console:
  * navigate to databases section of Cloud SQL and create a new database called bts
  * navigate to flights instance and select IMPORT
  * Specify location of create_table.sql in your bucket
  * Specify that you want to create a table in the database bts
* Load the CSV files into this table:
  * Browse to 201501.csv in your bucket
  * Specify CSV as the format
  * bts as the database
  * flights as the table
* In Cloud Shell, connect to database and run queries
  * Connect to the database using one of these two commands (the first if you don't need a SQL proxy, the second if you do -- you'll typically need a SQL proxy if your organization has set up a security rule to allow access only to authorized networks):
    * ```gcloud sql connect flights --user=postgres```
    * OR ```gcloud beta sql connect flights --user=postgres```
  * In the prompt, type ```\c bts;```
  * Type in the following query:
  ``` 
  SELECT "Origin", COUNT(*) AS num_flights 
  FROM flights GROUP BY "Origin" 
  ORDER BY num_flights DESC 
  LIMIT 5;
  ```
* Add more months of CSV data and notice that the performance degrades.
Once you are done, delete the Cloud SQL instance since you will not need it for the rest of the book.

### Creating view in BigQuery
* Run the script 
  ```./create_views.sh```
* Compute the contingency table for various thresholds by running the script 
  ```
  ./contingency.sh
  ```

### Building a dashboard
Follow the steps in the main text of the chapter to set up a Data Studio dashboard and create charts.

