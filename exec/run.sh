#!/bin/bash

# This bash script is adapted from https://github.com/bu-cms/bucoffea/blob/master/bucoffea/execute/htcondor_wrap.sh

echo "Currently in $PWD"
source $PWD/scripts/vomcheck.sh
source $PWD/dirsetup.sh $1
source $PWD/setup.sh $1

if [ ! -z "${VIRTUAL_ENV}" ]; then
    echo "Found environmental variable."
    if [ "$VIRTUAL_ENV" == "${ENV_NAME}" ]; then
        source ${VIRTUAL_ENV}/bin/activate
        export PYTHONPATH=$VIRTUAL_ENV/lib/python3.9/site-packages:$PYTHONPATH
    else
        source scripts/envsetup.sh
    fi
else
    source scripts/envsetup.sh
fi

echo "My python path is $PYTHONPATH"

python3 src/main.py
source scripts/transfer.sh $PROCESS_NAME
