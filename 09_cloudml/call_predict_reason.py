#!/usr/bin/env python

from googleapiclient import discovery
from oauth2client.client import GoogleCredentials
import json

credentials = GoogleCredentials.get_application_default()
api = discovery.build('ml', 'v1', credentials=credentials,
            discoveryServiceUrl='https://storage.googleapis.com/cloud-ml/discovery/ml_v1_discovery.json')

request_data = {'instances':
  [
      {
        'dep_delay': dep_delay,
        'taxiout': taxiout,
        'distance': 160.0,
        'avg_dep_delay': 13.34,
        'avg_arr_delay': avg_arr_delay,
        'carrier': 'AS',
        'dep_lat': 61.17,
        'dep_lon': -150.00,
        'arr_lat': 60.49,
        'arr_lon': -145.48,
        'origin': 'ANC',
        'dest': 'CDV'
      }
      for dep_delay, taxiout, avg_arr_delay in
        [[16.0, 13.0, 67.0],
         [13.3, 13.0, 67.0], # if dep_delay was the airport mean
         [16.0, 16.0, 67.0], # if taxiout was the global mean
         [16.0, 13.0, 4] # if avg_arr_delay was the global mean
        ]
  ]
}

PROJECT = 'cloud-training-demos'
parent = 'projects/%s/models/%s/versions/%s' % (PROJECT, 'flights', 'v1')
response = api.projects().predict(body=request_data, name=parent).execute()
print "response={0}".format(response)

probs = [pred[u'probabilities'][1] for pred in response[u'predictions']]
print "probs={0}".format(probs)
