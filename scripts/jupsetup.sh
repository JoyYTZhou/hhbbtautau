#!/usr/bin/bash
# ===========================================================================================================
# This script sets up the environment for jupyter notebook for testing, provided that the virtual environment
# containing with the proper packages installed is available
# Takes one argument: Process name
# Run this script only to open jupyter notebook
# ===========================================================================================================
export ENV_NAME=coffeajup
export ENV_FOR_DYNACONF=LPCJUPYTER

source scripts/envutil.sh
setup_LCG

source ~/nobackup/${ENV_NAME}/bin/activate

export PYTHONPATH=~/nobackup/${ENV_NAME}/lib/python3.9/site-packages:$PYTHONPATH
export PYTHONPATH=$PWD/src:$PYTHONPATH

export PATH=$(remove_duplicates "$PATH")
export PYTHONPATH=$(remove_duplicates "$PYTHONPATH")

alias jup='jupyter notebook --no-browser --port=2001'





