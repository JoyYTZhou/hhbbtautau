#!/bin/bash

function check_file {
    FILENAME=$1
    dasgoclient --query="site file=${FILENAME}"
    xrdfs root://cmsxrootd.fnal.gov/ ls -l $FILENAME
    xrdfs cms-xrd-global.cern.ch locate $FILENAME
}

function check_size {
    REDIRECTOR=$1
    FILENAME=$2
    output=$(xrdfs $REDIRECTOR stat $FILENAME)
    size=$(echo "$output" | grep -oP 'Size:\s+\K\d+')
}
