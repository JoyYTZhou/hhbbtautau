#!/bin/bash
# ===========================================================================================================
# This script sets up the environment to run this repository on lpc
# ===========================================================================================================

source scripts/sasetup.sh
export PYTHONPATH=~/nobackup/newcoffea/lib/python3.9/site-packages:$PYTHONPATH
source ~/nobackup/newcoffea/bin/activate

export PYTHONPATH=$PWD/src:$PYTHONPATH
source scripts/cleanpath.sh

export ENV_FOR_DYNACONF=LPCCONDOR
