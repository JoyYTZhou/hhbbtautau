#!/bin/bash

# =================================================================
# Run this script before submitting condor jobs
# =================================================================
source ../lpcsetup.sh
# if receiving arguments <datasetname>
if [ ! -z "$1" ]; then
    eosrm -r $SHORTPATH/$1
    eosmkdir -p $SHORTPATH/$1/cutflow
    eosmkdir -p $SHORTPATH/$1/object
else
    eosrm -r $SHORTPATH/backup
    eosmkdir -p $SHORTPATH/backup/cutflow
    eosmkdir -p $SHORTPATH/backup/object
fi