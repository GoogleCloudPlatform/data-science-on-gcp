#!/bin/bash
if [ "$YEAR" = "" ]
then
	export YEAR=2015
fi
echo "Ingesting YEAR=$YEAR..."
bash download.sh
bash zip_to_csv.sh
bash quotes_comma.sh
bash upload.sh
