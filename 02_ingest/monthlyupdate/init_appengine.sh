#!/bin/sh
gcloud app create --region us-central

git clone https://github.com/GoogleCloudPlatform/python-docs-samples
cd python-docs-samples/appengine/standard/hello_world

gcloud app deploy --quiet --stop-previous-version

