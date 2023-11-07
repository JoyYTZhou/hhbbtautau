#! /bin/bash

export PYTHONPATH=$PWD/src:$PYTHONPATH
export HHBBTT=$PWD

# MUST set the path to where you store the output before running this script
export OUTPUTPATH=$DATAPATH/output

rm -r $OUTPUTPATH
mkdir $OUTPUTPATH
mkdir $OUTPUTPATH/cutflow
mkdir $OUTPUTPATH/object


