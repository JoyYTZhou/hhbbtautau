#!/bin/bash

# This bash script is adapted from https://github.com/bu-cms/bucoffea/blob/master/bucoffea/execute/htcondor_wrap.sh

export IS_CONDOR=true
echo "Currently in $PWD"
export HHBBTT=$PWD
source $HHBBTT/lpcsetup.sh
source $HHBBTT/scripts/venv.sh
export PYTHONPATH=$PWD/src:$PYTHONPATH
export PROCESS_NAME=$1
source $HHBBTT/setup.sh $1

if [ ! -z "${VIRTUAL_ENV}" ]; then
    echo "Found environmental variable."
    source ${VIRTUAL_ENV}/bin/activate
else
    tar xf *tgz
    rm -rvf *tgz
    sh scripts/envsetup.sh
fi

python3 src/main.py
source transfer.sh $PROCESS_NAME
