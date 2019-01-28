#!/bin/bash
sudo pip install timezonefinder pytz apache-beam 'apache-beam[gcp]'
sudo pip install --upgrade google-cloud-bigquery
sudo pip install --upgrade google-cloud-pubsub
#sudo pip install --force six==1.10  # downgrade as 1.11 breaks apitools
