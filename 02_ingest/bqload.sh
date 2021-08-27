#!/bin/bash

if test "$#" -ne 1; then
   echo "Usage: ./download.sh rawcsvfile"
   echo "   eg: ./download.sh 201501.csv"
   exit
fi

CSVFILE=$1

SCHEMA=$(head -1 raw/201501.csv | sed 's/,/:STRING,/g')
SCHEMA="${SCHEMA}:STRING"
echo $SCHEMA
