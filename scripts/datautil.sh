#!/bin/bash

function check_file {
    FILENAME=$1

    dasgoclient --query="site file=${FILENAME}"
    xrdfs root://cmsxrootd.fnal.gov/ ls -l $FILENAME
    xrdfs cms-xrd-global.cern.ch locate $FILENAME
}
