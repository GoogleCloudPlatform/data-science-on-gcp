#!/bin/bash

#export YEAR=${YEAR:=2015}
SOURCE=https://transtats.bts.gov/PREZIP

OUTDIR=raw
mkdir -p $OUTDIR

for YEAR in `seq 2019 2019`; do
for MONTH in `seq 1 12`; do

  FILE=On_Time_Reporting_Carrier_On_Time_Performance_1987_present_${YEAR}_${MONTH}.zip
  curl -k -o ${OUTDIR}/${FILE}  ${SOURCE}/${FILE}

done
done
