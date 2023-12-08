#!/bin/bash

# =================================================================
# Run this script before submitting condor jobs
# =================================================================
export IS_CONDOR=true
source lpcsetup.sh
export PREFIX=root://cmseos.fnal.gov
# if receiving arguments <datasetname>
if [ ! -z "$1" ]; then
    xrdfs $PREFIX rm $SHORTPATH/$1/cutflow/*.csv
    xrdfs $PREFIX rm $SHORTPATH/$1/object/*.csv
    xrdfs $PREFIX rmdir $SHORTPATH/$1/cutflow
    xrdfs $PREFIX rmdir $SHORTPATH/$1/object
    xrdfs $PREFIX rmdir $SHORTPATH/$1
    xrdfs $PREFIX mkdir -p $SHORTPATH/$1/cutflow
    echo "Making directory $SHORTPATH/$1/cutflow"
    xrdfs $PREFIX mkdir -p $SHORTPATH/$1/object
    echo "Making directory $SHORTPATH/$1/object"
else
    xrdfs $PREFIX rm -r $SHORTPATH/all
    xrdfs $PREFIX mkdir -p $SHORTPATH/all/cutflow
    xrdfs $PREFIX mkdir -p $SHORTPATH/all/object
fi

echo "CONDOR outputpath is $SHORTPATH"
