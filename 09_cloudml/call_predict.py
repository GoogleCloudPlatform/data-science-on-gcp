#!/usr/bin/env python

from googleapiclient import discovery
from oauth2client.client import GoogleCredentials
import json

credentials = GoogleCredentials.get_application_default()
api = discovery.build('ml', 'v1beta1', credentials=credentials,
            discoveryServiceUrl='https://storage.googleapis.com/cloud-ml/discovery/ml_v1beta1_discovery.json')

request_data = {'instances':
  [
      {
        'dep_delay': 66.0,
        'taxiout': 13.0,
        'distance': 160.0,
        'avg_dep_delay': 13.34,
        'avg_arr_delay': 67.0,
        'carrier': 'AS',
        'dep_lat': 61.17,
        'dep_lon': -150.00,
        'arr_lat': 60.49,
        'arr_lon': -145.48,
        'origin': 'ANC',
        'dest': 'CDV'
      }
  ]
}

PROJECT = 'cloud-training-demos'
parent = 'projects/%s/models/%s/versions/%s' % (PROJECT, 'flights', 'v1')
response = api.projects().predict(body=request_data, name=parent).execute()
print "response={0}".format(response)
