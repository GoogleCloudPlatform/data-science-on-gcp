#!/bin/bash

YEAR=2015

for MONTH in `seq 1 1`; do
   bash download.sh $YEAR $MONTH
done

bash upload.sh
