# 2. Ingesting data onto the Cloud

### Populate your bucket with the data you will need for the book
The simplest way to get the files you need is to copy it from my bucket:
* Go to the 02_ingest folder of the repo
* Run the program ./ingest_from_crsbucket.sh and specify your bucket name.

Alternately, you can ingest from the original source of the data and carry out the cleanup steps as described in the text:
* Go to the 02_ingest folder of the repo
* Change the BUCKET variable in upload.sh
* Execute ./ingest_2015.sh
* Execute monthlyupdate/ingest_flights.py specifying your bucket name, and with year of 2016 and month of 01.  Type monthlyupdate/ingest_flights.py --help to get usage help.
This will initialize your bucket with the input files corresponding to 2015 and January 2016. These files are needed to carry out the steps that come later in this book.

### [Optional] Scheduling monthly downloads
* Go to the 02_ingest/monthlyupdate folder in the repo.
* Initialize a default AppEngine application in your project by running ./init_appengine.sh.
* Open the file app.yaml and change the CLOUD_STORAGE_BUCKET to reflect the name of your bucket.
* Run ./deploy.sh to deploy the Cron service app.  This will take 5-10 minutes.
* Visit the GCP web console and navigate to the AppEngine section. You should see two services: one the default (which is just a Hello World application) and the other is the flights service.
* Click on the flights service, follow the link to ingest the data and you’ll find that your access is forbidden -- the ingest capability is available only to the Cron service (or from the GCP web console by clicking the “Run now” button in the task queues section of AppEngine). If you click on “Run now”, a few minutes later, you’ll see the next month’s data show up in the storage bucket.
* Stop the flights application -- we won’t need it any further.
