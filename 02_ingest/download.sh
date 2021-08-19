#!/bin/bash

export YEAR=${YEAR:=2015}
echo "Downloading YEAR=$YEAR..."

for MONTH in `seq 1 12`; do

MONTH2=$(printf "%02d" $MONTH)

echo ${YEAR}_${MONTH}  ${YEAR}_${MONTH2}

TMPDIR=$(mktemp -d)

ZIPFILE=${TMPDIR}/${YEAR}_${MONTH2}.zip
echo $ZIPFILE

curl -o $ZIPFILE \
  https://transtats.bts.gov/PREZIP/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_${YEAR}_${MONTH}.zip
unzip -d $TMPDIR $ZIPFILE


mv $TMPDIR/*.csv ./${YEAR}_${MONTH2}.csv
rm -rf $TMPDIR

done

