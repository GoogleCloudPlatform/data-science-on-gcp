#!/bin/bash

# install Google Python client on all nodes
apt-get -y update
apt-get install python-dev
apt-get install -y python-pip
pip install --upgrade google-api-python-client

# git clone on Master
USER=CHANGE_TO_USER_NAME # the username that dataproc runs as
ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
if [[ "${ROLE}" == 'Master' ]]; then
  cd home/$USER
  git clone https://github.com/GoogleCloudPlatform/data-science-on-gcp
  chown -R $USER data-science-on-gcp
fi
