#!/bin/bash

# This bash script is adapted from https://github.com/bu-cms/bucoffea/blob/master/bucoffea/execute/htcondor_wrap.sh

echo "Currently in $PWD"

export PROCESS_NAME=$1
export ENV_NAME=newcoffea_el9
OLD_ENV_NAME=/uscms_data/d3/joyzhou/${ENV_NAME}

if [ ! -z "${VIRTUAL_ENV}" ] && [ "$VIRTUAL_ENV" == "${ENV_NAME}" ]; then
    echo "Found environmental variable."
else
    mkdir ${ENV_NAME}
    tar -xzf ${ENV_NAME}.tar.gz -C .
    sed -i "s|${OLD_ENV_NAME}|${PWD}/${ENV_NAME}|g" ${ENV_NAME}/bin/activate
    sed -i "s|${OLD_ENV_NAME}|${PWD}/${ENV_NAME}|g" ${ENV_NAME}/bin/*
    export VIRTUAL_ENV=${ENV_NAME}
fi

source scripts/lpcsetup.sh
source scripts/envutil.sh

export CONDOR_BASE=/store/user/joyzhou
LCG_sasetup

source ${ENV_NAME}/bin/activate

export PYTHONPATH=$VIRTUAL_ENV/lib/python3.9/site-packages:$PYTHONPATH
export MPLCONFIGDIR=matplotlibconfig
export PYTHONPATH=$PWD/src:$PYTHONPATH
export PATH=$(remove_duplicates "$PATH")
export PYTHONPATH=$(remove_duplicates "$PYTHONPATH")

echo $PYTHONPATH

export ENV_FOR_DYNACONF=LPCCONDOR
export OUTPUT_BASE=$PWD
export DEBUG_MODE=true

echo "start executing main file"
checkproxy

python3 src/main.py
# python3 src/plot.py
