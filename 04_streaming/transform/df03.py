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


def addtimezone(lat, lon):
    try:
        import timezonefinder
        tf = timezonefinder.TimezoneFinder()
        return lat, lon, tf.timezone_at(lng=float(lon), lat=float(lat))
        # return (lat, lon, 'America/Los_Angeles') # FIXME
    except ValueError:
        return lat, lon, 'TIMEZONE'  # header


def as_utc(date, hhmm, tzone):
    try:
        if len(hhmm) > 0 and tzone is not None:
            import datetime, pytz
            loc_tz = pytz.timezone(tzone)
            loc_dt = loc_tz.localize(datetime.datetime.strptime(date, '%Y-%m-%d'), is_dst=False)
            # can't just parse hhmm because the data contains 2400 and the like ...
            loc_dt += datetime.timedelta(hours=int(hhmm[:2]), minutes=int(hhmm[2:]))
            utc_dt = loc_dt.astimezone(pytz.utc)
            return utc_dt.strftime('%Y-%m-%d %H:%M:%S')
        else:
            return ''  # empty string corresponds to canceled flights
    except ValueError as e:
        logging.exception('{} {} {}'.format(date, hhmm, tzone))
        raise e


def tz_correct(line, airport_timezones):
    fields = json.loads(line)
    try:
        # convert all times to UTC
        dep_airport_id = fields["ORIGIN_AIRPORT_SEQ_ID"]
        arr_airport_id = fields["DEST_AIRPORT_SEQ_ID"]
        dep_timezone = airport_timezones[dep_airport_id][2]
        arr_timezone = airport_timezones[arr_airport_id][2]

        for f in ["CRS_DEP_TIME", "DEP_TIME", "WHEELS_OFF"]:
            fields[f] = as_utc(fields["FL_DATE"], fields[f], dep_timezone)
        for f in ["WHEELS_ON", "CRS_ARR_TIME", "ARR_TIME"]:
            fields[f] = as_utc(fields["FL_DATE"], fields[f], arr_timezone)

        yield json.dumps(fields)
    except KeyError as e:
        logging.exception(" Ignoring " + line + " because airport is not known")


if __name__ == '__main__':
    with beam.Pipeline('DirectRunner') as pipeline:
        airports = (pipeline
                    | 'airports:read' >> beam.io.ReadFromText('airports.csv.gz')
                    | beam.Filter(lambda line: "United States" in line)
                    | 'airports:fields' >> beam.Map(lambda line: next(csv.reader([line])))
                    | 'airports:tz' >> beam.Map(lambda fields: (fields[0], addtimezone(fields[21], fields[26])))
                    )

        flights = (pipeline
                   | 'flights:read' >> beam.io.ReadFromText('flights_sample.json')
                   | 'flights:tzcorr' >> beam.FlatMap(tz_correct, beam.pvalue.AsDict(airports))
                   )

        flights | beam.io.textio.WriteToText('all_flights')
