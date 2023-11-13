#! /bin/bash

export PYTHONPATH=$PWD/src:$PYTHONPATH
export HHBBTT=$PWD

# MUST set the path $OUTPUTPATH to where you store the output before running this script

if [ -z "${IS_CONDOR}" ]; then
    rm -rv $OUTPUTPATH/cutflow
    rm -rv $OUTPUTPATH/object
    mkdir -pv $OUTPUTPATH/cutflow
    mkdir -pv $OUTPUTPATH/object
else
    if [ ! -z "$1" ]; then
        rm -rv $OUTPUTPATH/$1/cutflow
        rm -rv $OUTPUTPATH/$1/object
        mkdir -pv $OUTPUTPATH/$1/cutflow
        mkdir -pv $OUTPUTPATH/$1/object
        echo "full output directory on executation area is $OUTPUTPATH"
    else
        export OUTPUTPATH=$OUTPUTPATH/backup
        eosrm -r $SHORTPATH/backup
        eosmkdir -p $SHORTPATH/backup/cutflow
        eosmkdir -p $SHORTPATH/backup/object
    fi
fi





