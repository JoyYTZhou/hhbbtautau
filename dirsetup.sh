#!/bin/bash

# =================================================================
# Set up condor output directory
# =================================================================
export IS_CONDOR=true
source lpcsetup.sh
export PREFIX=root://cmseos.fnal.gov

# if receiving arguments <datasetname>
# check if condor directory exists
if [ ! -z "$1" ]; then
    DIRNAME=$SHORTPATH/$1
else
    DIRNAME=$SHORTPATH/all
fi

if xrdfs $PREFIX stat $DIRNAME >/dev/null 2>&1; then
    echo "the directory $DIRNAME already exists"
else
    echo "creating directory $DIRNAME."
    xrdfs $PREFIX mkdir -p $DIRNAME
fi

echo "CONDOR outputpath is $DIRNAME"
