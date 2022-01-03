#!/bin/bash
bq query --nouse_legacy_sql --format=sparse \
    "SELECT EVENT_DATA FROM dsongcp.flights_simevents WHERE EVENT_TYPE = 'wheelsoff' AND EVENT_TIME BETWEEN '2015-03-10T10:00:00' AND '2015-03-10T14:00:00' " \
    | grep FL_DATE \
    > simevents_sample.json


bq query --nouse_legacy_sql --format=json \
    "SELECT * FROM dsongcp.flights_tzcorr WHERE DEP_TIME BETWEEN '2015-03-10T10:00:00' AND '2015-03-10T14:00:00' " \
    | sed 's/\[//g' | sed 's/\]//g' | sed s'/\},/\}\n/g' \
    > alldata_sample.json
