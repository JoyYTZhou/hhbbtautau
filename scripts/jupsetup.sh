#!/usr/bin/bash
# ===========================================================================================================
# This script sets up the environment for jupyter notebook for testing, provided that the virtual environment
# containing with the proper packages installed is available
# Takes one argument: Process name
# Run this script only to open jupyter notebook
# ===========================================================================================================


export ENV_NAME=coffeajup
export ENV_FOR_DYNACONF=LPCJUPYTER

output=$(lsb_release -a)
release_version=$(echo "$output" | grep 'Release:' | awk '{print $2}')

if [ "$release_version" == "8.9" ]; then
    echo "Performing operations for release 8.9"
    source /cvmfs/sft.cern.ch/lcg/views/LCG_104/x86_64-centos8-gcc11-opt/setup.sh
elif [ "$release_version" == "9.3" ]; then
    echo "Performing operations for release 9.3"
    source /cvmfs/sft.cern.ch/lcg/views/LCG_104/x86_64-el9-gcc13-opt/setup.sh
else
    source /cvmfs/sft.cern.ch/lcg/views/LCG_104/x86_64-centos7-gcc11-opt/setup.sh
fi

source ~/nobackup/${ENV_NAME}/bin/activate

export PYTHONPATH=~/nobackup/${ENV_NAME}/lib/python3.9/site-packages:$PYTHONPATH
export PYTHONPATH=$PWD/src:$PYTHONPATH

source scripts/cleanpath.sh

alias jup='jupyter notebook --no-browser --port=2001'





