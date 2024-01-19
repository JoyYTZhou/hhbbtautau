#!/bin/bash

# This bash script is adapted from https://github.com/bu-cms/bucoffea/blob/master/bucoffea/execute/htcondor_wrap.sh

echo "Currently in $PWD"
source $PWD/scripts/vomcheck.sh
source $PWD/dirsetup.sh $1
source $PWD/setup.sh $1

export ENV_NAME=newcoffea
OLD_ENV_NAME=/uscms_data/d3/joyzhou/${ENV_NAME}
if [ ! -z "${VIRTUAL_ENV}" ] && [ "$VIRTUAL_ENV" == "${ENV_NAME}" ]; then
    echo "Found environmental variable."
else
    mkdir ${ENV_NAME}
    tar -xzf ${ENV_NAME}.tar.gz -C .
    sed -i "s|${OLD_ENV_NAME}|${PWD}/${ENV_NAME}|g" ${ENV_NAME}/bin/activate
    sed -i "s|${OLD_ENV_NAME}|${PWD}/${ENV_NAME}|g" ${ENV_NAME}/bin/*
fi

source ${ENV_NAME}/bin/activate
echo "Virtual environment is ${VIRTUAL_ENV}."
export PYTHONPATH=$VIRTUAL_ENV/lib/python3.9/site-packages:$PYTHONPATH

echo "My python path is $PYTHONPATH"

python3 src/main.py
source scripts/transfer.sh $PROCESS_NAME
