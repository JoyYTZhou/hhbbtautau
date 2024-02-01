#!/bin/bash

# =================================================================
# Run this script before submitting condor jobs
# =================================================================
export IS_CONDOR=true
source lpcsetup.sh
export PREFIX=root://cmseos.fnal.gov
# if receiving arguments <datasetname>
if [ ! -z "$1" ]; then
    DIRNAME=$SHORTPATH/$1
else
    DIRNAME=$SHORTPATH/all
fi

# check if directory already exists
if xrdfs $PREFIX stat $DIRNAME >/dev/null 2>&1; then
    echo "the directory $DIRNAME already exists"
else
    echo "creating directory $DIRNAME."
    xrdfs $PREFIX mkdir -p $DIRNAME

echo "CONDOR outputpath is $DIRNAME"
