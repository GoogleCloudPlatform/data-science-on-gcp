#!/usr/bin/env python3

import pandas as pd
import glob

INCOLS=[
    "FlightDate", "Reporting_Airline",
    "DOT_ID_Reporting_Airline", "IATA_CODE_Reporting_Airline",
    "Flight_Number_Reporting_Airline", "OriginAirportID", "OriginAirportSeqID", "OriginCityMarketID",
    "Origin", "DestAirportID", "DestAirportSeqID", "DestCityMarketID", "Dest",
    "CRSDepTime", "DepTime", "DepDelay",
    "TaxiOut", "WheelsOff", "WheelsOn", "TaxiIn",
    "CRSArrTime", "ArrTime", "ArrDelay",
    "Cancelled", "CancellationCode",
    "Diverted", "Distance"
]
OUTCOLS=(
        "FL_DATE,UNIQUE_CARRIER,AIRLINE_ID,CARRIER,FL_NUM,ORIGIN_AIRPORT_ID,ORIGIN_AIRPORT_SEQ_ID,ORIGIN_CITY_MARKET_ID"
        +",ORIGIN,DEST_AIRPORT_ID,DEST_AIRPORT_SEQ_ID,DEST_CITY_MARKET_ID,DEST,CRS_DEP_TIME,DEP_TIME,DEP_DELAY"
        +",TAXI_OUT,WHEELS_OFF,WHEELS_ON,TAXI_IN,CRS_ARR_TIME,ARR_TIME,ARR_DELAY,CANCELLED,CANCELLATION_CODE"
        +",DIVERTED,DISTANCE").split(',')

for name in glob.glob('*.csv'):
    # status
    print(name)

    # read only selected columns from the file
    df = pd.read_csv(name, usecols=INCOLS)

    # replace file, renaming the columns
    df.to_csv(name, header=OUTCOLS, index=False)
