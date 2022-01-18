#!/bin/bash

python3 -m pip install --upgrade pip
python3 -m pip cache purge
python3 -m pip install --upgrade timezonefinder pytz 'apache-beam[gcp]'

echo "If this script fails, please try installing it in a virtualenv"
echo "virtualenv ~/beam_env"
echo "source ~/beam_env/bin/activate"
echo "./install_packages.sh"
