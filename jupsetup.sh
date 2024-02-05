#!/usr/bin/bash
# =================================================================
# This script sets up the environment for jupyter notebook
# Takes one argument: Process name
# =================================================================

OUTPUTPATH="/uscms_data/d1/joyzhou/output"

if [ ! -z "$1" ]; then
    export OUTPUTPATH=$OUTPUTPATH/$1
    export PROCESS_NAME=$1
else
    export OUTPUTPATH=$OUTPUTPATH/all
    export PROCESS_NAME=all
fi

echo "OUTPUTPATH has been set to: ${OUTPUTPATH}"
echo "PROCESS_NAME has been set to: ${PROCESS_NAME}"

if [ -d "$OUTPUTPATH" ]; then
    echo "the directory $OUTPUTPATH exists."
else
    echo "the directory $OUTPUTPATH does not exist."
    mkdir -pv $OUTPUTPATH
fi

export ENV_NAME=coffeajup
source /cvmfs/sft.cern.ch/lcg/views/LCG_104/x86_64-centos7-gcc11-opt/setup.sh
source ~/nobackup/${ENV_NAME}/bin/activate

export HHBBTT=$PWD
export PYTHONPATH=~/nobackup/${ENV_NAME}/lib/python3.9/site-packages:$PYTHONPATH
export PYTHONPATH=$HHBBTT/src:$PYTHONPATH
echo "HHBBTT has been set to: ${HHBBTT}"

source scripts/cleanpath.sh

jupyter notebook --no-browser --port=2001





