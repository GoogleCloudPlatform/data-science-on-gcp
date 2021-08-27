#!/bin/bash

SOURCE=https://storage.googleapis.com/data-science-on-gcp/edition2/raw
#SOURCE=https://transtats.bts.gov/PREZIP

if test "$#" -ne 2; then
   echo "Usage: ./download.sh year month"
   echo "   eg: ./download.sh 2015 1"
   exit
fi

YEAR=$1
MONTH=$2
echo "Downloading YEAR=$YEAR ...  MONTH=$MONTH ..."


MONTH2=$(printf "%02d" $MONTH)

TMPDIR=$(mktemp -d)

ZIPFILE=${TMPDIR}/${YEAR}_${MONTH2}.zip
echo $ZIPFILE

curl -o $ZIPFILE \
  ${SOURCE}/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_${YEAR}_${MONTH}.zip
unzip -d $TMPDIR $ZIPFILE

mv $TMPDIR/*.csv ./${YEAR}${MONTH2}.csv
rm -rf $TMPDIR
