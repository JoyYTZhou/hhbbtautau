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
# if submmitting batch jobs
else
    echo "submitting batch jobs"
    export OUTPUTPATH=$PWD/outputs
    source scripts/sasetup.sh
fi

export CONDORPATH="root://cmseos.fnal.gov//store/user/joyzhou/output"
export SHORTPATH=/store/user/joyzhou/output
echo "shortname for condor output path is $SHORTPATH"
echo "Output directory is ${OUTPUTPATH}"

source scripts/cleanpath.sh
export PYTHONPATH=$PWD/src:$PYTHONPATH
export HHBBTT=$PWD

if [ ! -z "$1" ]; then
    export OUTPUTPATH=$OUTPUTPATH/$1
    export PROCESS_NAME=$1
else
    export OUTPUTPATH=$OUTPUTPATH/all
    export PROCESS_NAME=all
fi

if [ -d "$OUTPUTPATH" ]; then
    echo "the directory $OUTPUTPATH exists."
else
    echo "the directory $OUTPUTPATH does not exist."
    mkdir -pv $OUTPUTPATH
fi




