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
import zipfile
import datetime
import tempfile
from pathlib import Path
from google.cloud import storage
from google.cloud.storage import Blob
from google.cloud import bigquery

SOURCE = "https://storage.googleapis.com/data-science-on-gcp/edition2/raw"
#SOURCE = "https://transtats.bts.gov/PREZIP"


def urlopen(url):
    """Open a URL with SSL context that allows older cipher suites.
    
    Args:
        url: The URL to open.
        
    Returns:
        Response object from urlopen.
    """
    from urllib.request import urlopen as impl
    import ssl
    ctx_no_secure = ssl.create_default_context()
    ctx_no_secure.set_ciphers('HIGH:!DH:!aNULL')
    ctx_no_secure.check_hostname = False
    ctx_no_secure.verify_mode = ssl.CERT_NONE
    return impl(url, context=ctx_no_secure)


def download(year: str, month: str, destdir: str):
    """Download on-time performance data from BTS and save to local file.
    
    Args:
        year: Year as string, e.g., '2015'.
        month: Month as string, e.g., '01' for January.
        destdir: Destination directory for the downloaded file.
        
    Returns:
        Path to the downloaded zip file.
    """
    logging.info(f'Requesting data for {year}-{month}-*')
    url = os.path.join(SOURCE,
                       f"On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{year}_{int(month)}.zip")
    logging.debug(f"Trying to download {url}")
    filename = os.path.join(destdir, f"{year}{month}.zip")
    with open(filename, "wb") as fp:
        response = urlopen(url)
        fp.write(response.read())
    logging.debug(f"{filename} saved")
    return filename


def zip_to_csv(filename, destdir):
    """Extract CSV file from zip archive and compress it with gzip.
    
    Args:
        filename: Path to the zip file.
        destdir: Directory where the CSV should be extracted.
        
    Returns:
        Path to the gzipped CSV file.
    """
    zip_ref = zipfile.ZipFile(filename, 'r')
    zip_ref.extractall(path=destdir)
    csvfile = Path(destdir) / zip_ref.namelist()[0]
    zip_ref.close()
    logging.info(f"Extracted {csvfile}")
    
    # now gzip for faster upload to bucket
    gzipped = str(csvfile) + ".gz"
    with open(csvfile, 'rb') as ifp:
        with gzip.open(gzipped, 'wb') as ofp:
            shutil.copyfileobj(ifp, ofp)
    logging.info(f"Compressed into {gzipped}")
    return gzipped


def upload(csvfile, bucketname, blobname):
    """Upload a CSV file to Google Cloud Storage.
    
    Args:
        csvfile: Path to the local CSV file.
        bucketname: Name of the GCS bucket.
        blobname: Destination blob name in the bucket.
        
    Returns:
        GCS location URI (gs://bucket/blob).
    """
    client = storage.Client()
    bucket = client.get_bucket(bucketname)
    logging.info(bucket)
    blob = Blob(blobname, bucket)
    logging.debug(f'Uploading {csvfile} ...')
    blob.upload_from_filename(csvfile)
    gcslocation = f'gs://{bucketname}/{blobname}'
    logging.info(f'Uploaded {gcslocation} ...')
    return gcslocation


def bqload(gcsfile, year, month):
    """Load CSV file from GCS into BigQuery, replacing existing partition data.
    
    Args:
        gcsfile: GCS URI of the file to load.
        year: Year as string for partition identification.
        month: Month as string for partition identification.
        
    Returns:
        Tuple of (table_ref, number_of_rows_loaded).
    """
    client = bigquery.Client()
    # truncate existing partition ...
    table_ref = client.dataset('dsongcp').table(f'flights_raw${year}{month}')
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
    """Ingest flights data from BTS website to Google Cloud Storage and BigQuery.
    
    Downloads flight data for the specified year and month, uploads to GCS,
    and loads into BigQuery.
    
    Args:
        year: Year as string, e.g., '2015'.
        month: Month as string, e.g., '01' for January.
        bucket: GCS bucket name for storage.
        
    Returns:
        Tuple of (table_ref, number_of_rows) on success.
        
    Raises:
        Exception: If data is not available on BTS website or processing fails.
    """
    tempdir = tempfile.mkdtemp(prefix='ingest_flights')
    try:
        zipfile = download(year, month, tempdir)
        bts_csv = zip_to_csv(zipfile, tempdir)
        gcsloc = f'flights/raw/{year}{month}.csv.gz'
        gcsloc = upload(bts_csv, bucket, gcsloc)
        return bqload(gcsloc, year, month)
    finally:
        logging.debug(f'Cleaning up by removing {tempdir}')
        shutil.rmtree(tempdir)


def next_month(bucketname):
    """Determine the next month to download based on existing GCS files.
    
    Scans the GCS bucket for existing flight data files and calculates
    the next month that should be downloaded.
    
    Args:
        bucketname: Name of the GCS bucket to check.
        
    Returns:
        Tuple of (year, month) strings for the next month to download.
    """
    client = storage.Client()
    bucket = client.get_bucket(bucketname)
    blobs = list(bucket.list_blobs(prefix='flights/raw/'))
    files = [blob.name for blob in blobs if 'csv' in blob.name]  # csv files only
    lastfile = os.path.basename(files[-1])
    logging.debug(f'The latest file on GCS is {lastfile}')
    year = lastfile[:4]
    month = lastfile[4:6]
    return compute_next_month(year, month)


def compute_next_month(year, month):
    """Calculate the next month given a year and month.
    
    Args:
        year: Year as string.
        month: Month as string.
        
    Returns:
        Tuple of (year, month) strings for the next month.
    """
    dt = datetime.datetime(int(year), int(month), 15)  # 15th of month
    dt = dt + datetime.timedelta(30)  # will always go to next month
    logging.debug(f'The next month is {dt}')
    return f'{dt.year}', f'{dt.month:02d}'


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

        logging.debug(f'Ingesting year={year_} month={month_}')
        tableref, numrows = ingest(year_, month_, args.bucket)
        logging.info(f'Success ... ingested {numrows} rows to {tableref}')
    except Exception as e:
        logging.exception("Try again later?")
