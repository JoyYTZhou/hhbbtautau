#!/bin/bash

# =================================================================
# Run this script before submitting condor jobs
# =================================================================
export IS_CONDOR=true
source lpcsetup.sh
export PREFIX=root://cmseos.fnal.gov
# if receiving arguments <datasetname>
if [ ! -z "$1" ]; then
    xrdfs $PREFIX rm $SHORTPATH/$1/*.csv
    xrdfs $PREFIX rm $SHORTPATH/$1/*.csv
    xrdfs $PREFIX rmdir $SHORTPATH/$1
    xrdfs $PREFIX rmdir $SHORTPATH/$1
    xrdfs $PREFIX rmdir $SHORTPATH/$1
    xrdfs $PREFIX mkdir -p $SHORTPATH/$1
    echo "Making directory $SHORTPATH/$1"
    xrdfs $PREFIX mkdir -p $SHORTPATH/$1
    echo "Making directory $SHORTPATH/$1"
else
    xrdfs $PREFIX rm -r $SHORTPATH/all
    xrdfs $PREFIX mkdir -p $SHORTPATH/all
    xrdfs $PREFIX mkdir -p $SHORTPATH/all
fi

echo "CONDOR outputpath is $SHORTPATH"
