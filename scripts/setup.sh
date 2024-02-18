#! /bin/bash
# =================================================================
# This script sets up the environment in any execution machine
# Takes one argument: Process name
# =================================================================

# if not submitting batch jobs
if [ -z "${IS_CONDOR}" ]; then
    echo "Not submitting batch jobs"
    OUTPUTPATH="/uscms_data/d1/joyzhou/output"
    source scripts/venv.sh
    export SPAWN_CLIENT=true
    echo "Will spawn dask client!"
# if submmitting batch jobs
else
    echo "submitting batch jobs"
    export OUTPUTPATH=$PWD/outputs
    source scripts/sasetup.sh
fi

source scripts/cleanpath.sh
export HHBBTT=$PWD
export PYTHONPATH=$HHBBTT/src:$PYTHONPATH
echo "HHBBTT has been set to: ${HHBBTT}"

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




