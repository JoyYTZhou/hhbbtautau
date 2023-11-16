#!/bin/bash

source /cvmfs/sft.cern.ch/lcg/views/LCG_103swan/x86_64-centos7-gcc11-opt/setup.sh
python -m venv hhbbttrun
echo "creating new venv..."
source hhbbttrun/bin/activate
export ENV_NAME=hhbbttrun
python3 -m pip install coffea --upgrade --no-cache-dir
echo "Installed Coffea"
python3 -m pip install dynaconf --no-cache-dir
echo "Installed dyanconf"
export PYTHONPATH=$ENV_NAME/lib/python3.9/site-packages:$PYTHONPATH
