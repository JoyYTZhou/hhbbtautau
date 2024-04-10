#!/bin/bash

# This bash script is adapted from https://github.com/bu-cms/bucoffea/blob/master/bucoffea/execute/htcondor_wrap.sh

echo "Currently in $PWD"

export PROCESS_NAME=$1
export ENV_NAME=newcoffea
OLD_ENV_NAME=/uscms_data/d3/joyzhou/${ENV_NAME}

if [ ! -z "${VIRTUAL_ENV}" ] && [ "$VIRTUAL_ENV" == "${ENV_NAME}" ]; then
    echo "Found environmental variable."
else
    mkdir ${ENV_NAME}
    tar -xzf ${ENV_NAME}.tar.gz -C .
    sed -i "s|${OLD_ENV_NAME}|${PWD}/${ENV_NAME}|g" ${ENV_NAME}/bin/activate
    sed -i "s|${OLD_ENV_NAME}|${PWD}/${ENV_NAME}|g" ${ENV_NAME}/bin/*
    export VIRTUAL_ENV=newcoffea
fi

source scripts/lpcsetup.sh
source scripts/envutil.sh

# version=x86_64-centos7-gcc11-opt
# source /cvmfs/sft.cern.ch/lcg/releases/LCG_104swan/Python/3.9.12/$version/Python-env.sh
# echo "Successfully sourced python package"
# source /cvmfs/sft.cern.ch/lcg/releases/LCG_104swan/ROOT/6.28.04/$version/ROOT-env.sh
# echo "Successfully sourced ROOT software"

LCG_sasetup

source ${ENV_NAME}/bin/activate

export PYTHONPATH=$VIRTUAL_ENV/lib/python3.9/site-packages:$PYTHONPATH
export MPLCONFIGDIR=matplotlibconfig
export PYTHONPATH=$PWD/src:$PYTHONPATH
export PATH=$(remove_duplicates "$PATH")
export PYTHONPATH=$(remove_duplicates "$PYTHONPATH")

echo $PYTHONPATH

export ENV_FOR_DYNACONF=LPCCONDOR

echo "start executing main file"
python3 src/main.py
# python3 src/plot.py
