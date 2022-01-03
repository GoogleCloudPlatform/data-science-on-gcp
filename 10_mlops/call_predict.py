# Copyright 2021 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys, json
from google.cloud import aiplatform
from google.cloud.aiplatform import gapic as aip

ENDPOINT_NAME = 'flights'

if __name__ == '__main__':

    endpoints = aiplatform.Endpoint.list(
        filter='display_name="{}"'.format(ENDPOINT_NAME),
        order_by='create_time desc'
    )
    if len(endpoints) == 0:
        print("No endpoint named {}".format(ENDPOINT_NAME))
        sys.exit(-1)
    
    endpoint = endpoints[0]
    
    input_data = {"instances": [
        {"dep_hour": 2, "is_weekday": 1, "dep_delay": 40, "taxi_out": 17, "distance": 41, "carrier": "AS",
         "dep_airport_lat": 58.42527778, "dep_airport_lon": -135.7075, "arr_airport_lat": 58.35472222,
         "arr_airport_lon": -134.57472222, "origin": "GST", "dest": "JNU"},
        {"dep_hour": 22, "is_weekday": 0, "dep_delay": -7, "taxi_out": 7, "distance": 201, "carrier": "HA",
         "dep_airport_lat": 21.97611111, "dep_airport_lon": -159.33888889, "arr_airport_lat": 20.89861111,
         "arr_airport_lon": -156.43055556, "origin": "LIH", "dest": "OGG"}
    ]}

    preds = endpoint.predict(input_data['instances'])
    print(preds)
    


