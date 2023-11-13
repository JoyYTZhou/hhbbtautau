#!/bin/bash

source /cvmfs/sft.cern.ch/lcg/views/LCG_103swan/x86_64-centos7-gcc11-opt/setup.sh
python -m venv hhbbttrun
source hhbbttrun/bin/activate
export PYTHONPATH=${ENVNAME}/lib/python3.9/site-packages:$PYTHONPATH
python3 -m pip install coffea --upgrade
python3 -m pip install dynaconf
