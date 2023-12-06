#! /bin/bash
# =================================================================
# This script sets up the environment in any execution machine
# Takes one argument: Process name
# =================================================================

export PYTHONPATH=$PWD/src:$PYTHONPATH
export HHBBTT=$PWD

# MUST set the path $OUTPUTPATH to where you store the output before running this script

if [ ! -z "$1" ]; then
    export OUTPUTPATH=$OUTPUTPATH/$1
    export PROCESS_NAME=$1
else
    export OUTPUTPATH=$OUTPUTPATH/all
    export PROCESS_NAME=all
fi

rm -rvf $OUTPUTPATH/cutflow
rm -rvf $OUTPUTPATH/object
mkdir -pv $OUTPUTPATH/cutflow
mkdir -pv $OUTPUTPATH/object
echo "full output directory on executation area is $OUTPUTPATH"



