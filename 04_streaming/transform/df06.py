#!/usr/bin/env python3

# Copyright 2016 Google Inc.
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

import apache_beam as beam
import logging
import csv
import json


DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S'


def addtimezone(lat, lon):
    try:
        import timezonefinder
        tf = timezonefinder.TimezoneFinder()
        lat = float(lat)
        lon = float(lon)
        return lat, lon, tf.timezone_at(lng=lon, lat=lat)
    except ValueError:
        return lat, lon, 'TIMEZONE'  # header


def as_utc(date, hhmm, tzone):
    """
    Returns date corrected for timezone, and the tzoffset
    """
    try:
        if len(hhmm) > 0 and tzone is not None:
            import datetime, pytz
            loc_tz = pytz.timezone(tzone)
            loc_dt = loc_tz.localize(datetime.datetime.strptime(date, '%Y-%m-%d'), is_dst=False)
            # can't just parse hhmm because the data contains 2400 and the like ...
            loc_dt += datetime.timedelta(hours=int(hhmm[:2]), minutes=int(hhmm[2:]))
            utc_dt = loc_dt.astimezone(pytz.utc)
            return utc_dt.strftime(DATETIME_FORMAT), loc_dt.utcoffset().total_seconds()
        else:
            return '', 0  # empty string corresponds to canceled flights
    except ValueError as e:
        logging.exception('{} {} {}'.format(date, hhmm, tzone))
        raise e


def add_24h_if_before(arrtime, deptime):
    import datetime
    if len(arrtime) > 0 and len(deptime) > 0 and arrtime < deptime:
        adt = datetime.datetime.strptime(arrtime, DATETIME_FORMAT)
        adt += datetime.timedelta(hours=24)
        return adt.strftime(DATETIME_FORMAT)
    else:
        return arrtime


def tz_correct(fields, airport_timezones):
    fields['FL_DATE'] = fields['FL_DATE'].strftime('%Y-%m-%d')  # convert to a string so JSON code works
    try:
        # convert all times to UTC
        dep_airport_id = fields["ORIGIN_AIRPORT_SEQ_ID"]
        arr_airport_id = fields["DEST_AIRPORT_SEQ_ID"]

        dep_timezone = airport_timezones[dep_airport_id][2]
        arr_timezone = airport_timezones[arr_airport_id][2]

        for f in ["CRS_DEP_TIME", "DEP_TIME", "WHEELS_OFF"]:
            fields[f], deptz = as_utc(fields["FL_DATE"], fields[f], dep_timezone)
        for f in ["WHEELS_ON", "CRS_ARR_TIME", "ARR_TIME"]:
            fields[f], arrtz = as_utc(fields["FL_DATE"], fields[f], arr_timezone)

        for f in ["WHEELS_OFF", "WHEELS_ON", "CRS_ARR_TIME", "ARR_TIME"]:
            fields[f] = add_24h_if_before(fields[f], fields["DEP_TIME"])

        fields["DEP_AIRPORT_LAT"] = airport_timezones[dep_airport_id][0]
        fields["DEP_AIRPORT_LON"] = airport_timezones[dep_airport_id][1]
        fields["DEP_AIRPORT_TZOFFSET"] = deptz
        fields["ARR_AIRPORT_LAT"] = airport_timezones[arr_airport_id][0]
        fields["ARR_AIRPORT_LON"] = airport_timezones[arr_airport_id][1]
        fields["ARR_AIRPORT_TZOFFSET"] = arrtz
        yield fields
    except KeyError:
        #logging.exception(f"Ignoring {fields} because airport is not known")
        pass

    except KeyError:
        logging.exception("Ignoring field because airport is not known")


def get_next_event(fields):
    if len(fields["DEP_TIME"]) > 0:
        event = dict(fields)  # copy
        event["EVENT_TYPE"] = "departed"
        event["EVENT_TIME"] = fields["DEP_TIME"]
        for f in ["TAXI_OUT", "WHEELS_OFF", "WHEELS_ON", "TAXI_IN", "ARR_TIME", "ARR_DELAY", "DISTANCE"]:
            event.pop(f, None)  # not knowable at departure time
        yield event
    if len(fields["WHEELS_OFF"]) > 0:
        event = dict(fields)  # copy
        event["EVENT_TYPE"] = "wheelsoff"
        event["EVENT_TIME"] = fields["WHEELS_OFF"]
        for f in ["WHEELS_ON", "TAXI_IN", "ARR_TIME", "ARR_DELAY", "DISTANCE"]:
            event.pop(f, None)  # not knowable at departure time
        yield event
    if len(fields["ARR_TIME"]) > 0:
        event = dict(fields)
        event["EVENT_TYPE"] = "arrived"
        event["EVENT_TIME"] = fields["ARR_TIME"]
        yield event


def create_event_row(fields):
    featdict = dict(fields)  # copy
    featdict['EVENT_DATA'] = json.dumps(fields)
    return featdict


def run(project, bucket):
    argv = [
        '--project={0}'.format(project),
        '--staging_location=gs://{0}/flights/staging/'.format(bucket),
        '--temp_location=gs://{0}/flights/temp/'.format(bucket),
        '--runner=DirectRunner'
    ]
    airports_filename = 'gs://{}/flights/airports/airports.csv.gz'.format(bucket)
    flights_output = 'gs://{}/flights/tzcorr/all_flights'.format(bucket)

    with beam.Pipeline(argv=argv) as pipeline:
        airports = (pipeline
                    | 'airports:read' >> beam.io.ReadFromText(airports_filename)
                    | beam.Filter(lambda line: "United States" in line)
                    | 'airports:fields' >> beam.Map(lambda line: next(csv.reader([line])))
                    | 'airports:tz' >> beam.Map(lambda fields: (fields[0], addtimezone(fields[21], fields[26])))
                    )

        flights = (pipeline
                   | 'flights:read' >> beam.io.ReadFromBigQuery(
                    query='SELECT * FROM dsongcp.flights WHERE rand() < 0.001', use_standard_sql=True)
                   | 'flights:tzcorr' >> beam.FlatMap(tz_correct, beam.pvalue.AsDict(airports))
                   )

        (flights
         | 'flights:tostring' >> beam.Map(lambda fields: json.dumps(fields))
         | 'flights:gcsout' >> beam.io.textio.WriteToText(flights_output)
         )
        
        flights_schema = ','.join([
            'FL_DATE:date',
            'UNIQUE_CARRIER:string',
            'ORIGIN_AIRPORT_SEQ_ID:string',
            'ORIGIN:string',
            'DEST_AIRPORT_SEQ_ID:string',
            'DEST:string',
            'CRS_DEP_TIME:timestamp',
            'DEP_TIME:timestamp',
            'DEP_DELAY:float',
            'TAXI_OUT:float',
            'WHEELS_OFF:timestamp',
            'WHEELS_ON:timestamp',
            'TAXI_IN:float',
            'CRS_ARR_TIME:timestamp',
            'ARR_TIME:timestamp',
            'ARR_DELAY:float',
            'CANCELLED:boolean',
            'DIVERTED:boolean',
            'DISTANCE:float',
            'DEP_AIRPORT_LAT:float',
            'DEP_AIRPORT_LON:float',
            'DEP_AIRPORT_TZOFFSET:float',
            'ARR_AIRPORT_LAT:float',
            'ARR_AIRPORT_LON:float',
            'ARR_AIRPORT_TZOFFSET:float',
            'Year:string'])
        
        # autodetect on JSON works, but is less reliable
        #flights_schema = 'SCHEMA_AUTODETECT'
        
        (flights 
         | 'flights:bqout' >> beam.io.WriteToBigQuery(
                'dsongcp.flights_tzcorr', 
                schema=flights_schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
                )
        )
        
        events = flights | beam.FlatMap(get_next_event)
        events_schema = ','.join([flights_schema, 'EVENT_TYPE:string,EVENT_TIME:timestamp,EVENT_DATA:string'])

        (events
         | 'events:totablerow' >> beam.Map(lambda fields: create_event_row(fields))
         | 'events:bqout' >> beam.io.WriteToBigQuery(
                'dsongcp.flights_simevents', schema=events_schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
                )
        )
        
if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Run pipeline on the cloud')
    parser.add_argument('-p', '--project', help='Unique project ID', required=True)
    parser.add_argument('-b', '--bucket', help='Bucket where gs://BUCKET/flights/airports/airports.csv.gz exists',
                        required=True)

    args = vars(parser.parse_args())

    print("Correcting timestamps and writing to BigQuery dataset")

    run(project=args['project'], bucket=args['bucket'])
