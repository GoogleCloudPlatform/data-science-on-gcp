#!/usr/bin/env python3

# Copyright 2016-2021 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import gzip
import shutil
import logging
import os.path
import zipfile
import datetime
import tempfile
from google.cloud import storage
from google.cloud.storage import Blob
from google.cloud import bigquery

SOURCE = "https://storage.googleapis.com/data-science-on-gcp/edition2/raw"
#SOURCE = "https://transtats.bts.gov/PREZIP"


def urlopen(url):
    from urllib.request import urlopen as impl
    import ssl

    ctx_no_secure = ssl.create_default_context()
    ctx_no_secure.set_ciphers('HIGH:!DH:!aNULL')
    ctx_no_secure.check_hostname = False
    ctx_no_secure.verify_mode = ssl.CERT_NONE
    return impl(url, context=ctx_no_secure)


def download(year: str, month: str, destdir: str):
    """
    Downloads on-time performance data and returns local filename
    year e.g.'2015'
    month e.g. '01 for January
    """
    logging.info('Requesting data for {}-{}-*'.format(year, month))

    url = os.path.join(SOURCE,
                       "On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{}_{}.zip".format(year, int(month)))
    logging.debug("Trying to download {}".format(url))

    filename = os.path.join(destdir, "{}{}.zip".format(year, month))
    with open(filename, "wb") as fp:
        response = urlopen(url)
        fp.write(response.read())
    logging.debug("{} saved".format(filename))
    return filename


def zip_to_csv(filename, destdir):
    """
    Extracts the CSV file from the zip file into the destdir
    """
    zip_ref = zipfile.ZipFile(filename, 'r')
    cwd = os.getcwd()
    os.chdir(destdir)
    zip_ref.extractall()
    os.chdir(cwd)
    csvfile = os.path.join(destdir, zip_ref.namelist()[0])
    zip_ref.close()
    logging.info("Extracted {}".format(csvfile))

    # now gzip for faster upload to bucket
    gzipped = csvfile + ".gz"
    with open(csvfile, 'rb') as ifp:
        with gzip.open(gzipped, 'wb') as ofp:
            shutil.copyfileobj(ifp, ofp)
    logging.info("Compressed into {}".format(gzipped))

    return gzipped


def upload(csvfile, bucketname, blobname):
    """
    Uploads the CSV file into the bucket with the given blobname
    """
    client = storage.Client()
    bucket = client.get_bucket(bucketname)
    logging.info(bucket)
    blob = Blob(blobname, bucket)
    logging.debug('Uploading {} ...'.format(csvfile))
    blob.upload_from_filename(csvfile)
    gcslocation = 'gs://{}/{}'.format(bucketname, blobname)
    logging.info('Uploaded {} ...'.format(gcslocation))
    return gcslocation


def bqload(gcsfile, year, month):
    """
    Loads the CSV file in GCS into BigQuery, replacing the existing data in that partition
    """
    client = bigquery.Client()
    # truncate existing partition ...
    table_ref = client.dataset('dsongcp').table('flights_raw${}{}'.format(year, month))
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = 'CSV'
    job_config.write_disposition = 'WRITE_TRUNCATE'
    job_config.ignore_unknown_values = True
    job_config.time_partitioning = bigquery.table.TimePartitioning('MONTH', 'FlightDate')
    job_config.skip_leading_rows = 1
    job_config.schema = [
        bigquery.SchemaField(col_and_type.split(':')[0], col_and_type.split(':')[1])  #, mode='required')
        for col_and_type in
        "Year:STRING,Quarter:STRING,Month:STRING,DayofMonth:STRING,DayOfWeek:STRING,FlightDate:DATE,Reporting_Airline:STRING,DOT_ID_Reporting_Airline:STRING,IATA_CODE_Reporting_Airline:STRING,Tail_Number:STRING,Flight_Number_Reporting_Airline:STRING,OriginAirportID:STRING,OriginAirportSeqID:STRING,OriginCityMarketID:STRING,Origin:STRING,OriginCityName:STRING,OriginState:STRING,OriginStateFips:STRING,OriginStateName:STRING,OriginWac:STRING,DestAirportID:STRING,DestAirportSeqID:STRING,DestCityMarketID:STRING,Dest:STRING,DestCityName:STRING,DestState:STRING,DestStateFips:STRING,DestStateName:STRING,DestWac:STRING,CRSDepTime:STRING,DepTime:STRING,DepDelay:STRING,DepDelayMinutes:STRING,DepDel15:STRING,DepartureDelayGroups:STRING,DepTimeBlk:STRING,TaxiOut:STRING,WheelsOff:STRING,WheelsOn:STRING,TaxiIn:STRING,CRSArrTime:STRING,ArrTime:STRING,ArrDelay:STRING,ArrDelayMinutes:STRING,ArrDel15:STRING,ArrivalDelayGroups:STRING,ArrTimeBlk:STRING,Cancelled:STRING,CancellationCode:STRING,Diverted:STRING,CRSElapsedTime:STRING,ActualElapsedTime:STRING,AirTime:STRING,Flights:STRING,Distance:STRING,DistanceGroup:STRING,CarrierDelay:STRING,WeatherDelay:STRING,NASDelay:STRING,SecurityDelay:STRING,LateAircraftDelay:STRING,FirstDepTime:STRING,TotalAddGTime:STRING,LongestAddGTime:STRING,DivAirportLandings:STRING,DivReachedDest:STRING,DivActualElapsedTime:STRING,DivArrDelay:STRING,DivDistance:STRING,Div1Airport:STRING,Div1AirportID:STRING,Div1AirportSeqID:STRING,Div1WheelsOn:STRING,Div1TotalGTime:STRING,Div1LongestGTime:STRING,Div1WheelsOff:STRING,Div1TailNum:STRING,Div2Airport:STRING,Div2AirportID:STRING,Div2AirportSeqID:STRING,Div2WheelsOn:STRING,Div2TotalGTime:STRING,Div2LongestGTime:STRING,Div2WheelsOff:STRING,Div2TailNum:STRING,Div3Airport:STRING,Div3AirportID:STRING,Div3AirportSeqID:STRING,Div3WheelsOn:STRING,Div3TotalGTime:STRING,Div3LongestGTime:STRING,Div3WheelsOff:STRING,Div3TailNum:STRING,Div4Airport:STRING,Div4AirportID:STRING,Div4AirportSeqID:STRING,Div4WheelsOn:STRING,Div4TotalGTime:STRING,Div4LongestGTime:STRING,Div4WheelsOff:STRING,Div4TailNum:STRING,Div5Airport:STRING,Div5AirportID:STRING,Div5AirportSeqID:STRING,Div5WheelsOn:STRING,Div5TotalGTime:STRING,Div5LongestGTime:STRING,Div5WheelsOff:STRING,Div5TailNum:STRING".split(',')
    ]
    load_job = client.load_table_from_uri(gcsfile, table_ref, job_config=job_config)
    load_job.result()  # waits for table load to complete

    if load_job.state != 'DONE':
        raise load_job.exception()

    return table_ref, load_job.output_rows


def ingest(year, month, bucket):
    '''
   ingest flights data from BTS website to Google Cloud Storage
   return table, numrows on success.
   raises exception if this data is not on BTS website
   '''
    tempdir = tempfile.mkdtemp(prefix='ingest_flights')
    try:
        zipfile = download(year, month, tempdir)
        bts_csv = zip_to_csv(zipfile, tempdir)
        gcsloc = 'flights/raw/{}{}.csv.gz'.format(year, month)
        gcsloc = upload(bts_csv, bucket, gcsloc)
        return bqload(gcsloc, year, month)
    finally:
        logging.debug('Cleaning up by removing {}'.format(tempdir))
        shutil.rmtree(tempdir)


def next_month(bucketname):
    '''
     Finds which months are on GCS, and returns next year,month to download
   '''
    client = storage.Client()
    bucket = client.get_bucket(bucketname)
    blobs = list(bucket.list_blobs(prefix='flights/raw/'))
    files = [blob.name for blob in blobs if 'csv' in blob.name]  # csv files only
    lastfile = os.path.basename(files[-1])
    logging.debug('The latest file on GCS is {}'.format(lastfile))
    year = lastfile[:4]
    month = lastfile[4:6]
    return compute_next_month(year, month)


def compute_next_month(year, month):
    dt = datetime.datetime(int(year), int(month), 15)  # 15th of month
    dt = dt + datetime.timedelta(30)  # will always go to next month
    logging.debug('The next month is {}'.format(dt))
    return '{}'.format(dt.year), '{:02d}'.format(dt.month)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='ingest flights data from BTS website to Google Cloud Storage')
    parser.add_argument('--bucket', help='GCS bucket to upload data to', required=True)
    parser.add_argument('--year', help='Example: 2015.  If not provided, defaults to getting next month')
    parser.add_argument('--month', help='Specify 01 for January. If not provided, defaults to getting next month')
    parser.add_argument('--debug', dest='debug', action='store_true', help='Specify if you want debug messages')

    try:
        args = parser.parse_args()
        if args.debug:
            logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.DEBUG)
        else:
            logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)

        if args.year is None or args.month is None:
            year_, month_ = next_month(args.bucket)
        else:
            year_ = args.year
            month_ = args.month
        logging.debug('Ingesting year={} month={}'.format(year_, month_))
        tableref, numrows = ingest(year_, month_, args.bucket)
        logging.info('Success ... ingested {} rows to {}'.format(numrows, tableref))
    except Exception as e:
        logging.exception("Try again later?")
