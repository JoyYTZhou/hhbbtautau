#!/bin/bash

# =================================================================
# Run this script before submitting condor jobs
# =================================================================
export IS_CONDOR=true
source ../lpcsetup.sh
export PREFIX=root://cmseos.fnal.gov
# if receiving arguments <datasetname>
if [ ! -z "$1" ]; then
    xrdfs $PREFIX rm -r $SHORTPATH/$1
    xrdfs $PREFIX mkdir -p $SHORTPATH/$1/cutflow
    echo "Making directory $SHORTPATH/$1/cutflow"
    xrdfs $PREFIX mkdir -p $SHORTPATH/$1/object
    echo "Making directory $SHORTPATH/$1/object"
else
    xrdfs $PREFIX rm -r $SHORTPATH/backup
    xrdfs $PREFIX mkdir -p $SHORTPATH/backup/cutflow
    xrdfs $PREFIX mkdir -p $SHORTPATH/backup/object
fi

echo "CONDOR outputpath is $SHORTPATH"
