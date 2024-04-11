#!/bin/bash
# ===========================================================================================================
# This script sets up the environment to run this repository on lpc
# ===========================================================================================================

source scripts/envutil.sh
setup_LCG
ENV_NAME=newcoffea_el9

export PYTHONPATH=~/nobackup/${ENV_NAME}/lib/python3.9/site-packages:$PYTHONPATH
source ~/nobackup/${ENV_NAME}/bin/activate
export PYTHONPATH=$PWD/src:$PYTHONPATH

export PATH=$(remove_duplicates "$PATH")
export PYTHONPATH=$(remove_duplicates "$PYTHONPATH")

export ENV_FOR_DYNACONF=LPCTEST
