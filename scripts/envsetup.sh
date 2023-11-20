#!/bin/bash

source scripts/sasetup.sh
python -m venv hhbbttrun
echo "creating new venv..."
source hhbbttrun/bin/activate
export ENV_NAME=hhbbttrun
python3 -m pip install coffea --upgrade --no-cache-dir
echo "Installed Coffea"
python3 -m pip install dynaconf --no-cache-dir
echo "Installed dyanconf"
python3 -m pip install vector --upgrade --no-cache-dir
echo "Installed vector"
export PYTHONPATH=$ENV_NAME/lib/python3.9/site-packages:$PYTHONPATH
