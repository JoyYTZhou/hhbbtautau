#!/bin/bash

source scripts/sasetup.sh
python -m venv $1/hhbbttrun
echo "creating new venv..."
source $1/hhbbttrun/bin/activate
export ENV_NAME=hhbbttrun
python3 -m pip install coffea --upgrade --no-cache-dir
echo "Installed Coffea"
python3 -m pip install dynaconf --no-cache-dir
echo "Installed dyanconf"
python3 -m pip install vector --upgrade --no-cache-dir
echo "Installed vector"
deactivate

# create tarball
tar -czvf ${ENV_NAME}.tar.gz $1/${ENV_NAME}


