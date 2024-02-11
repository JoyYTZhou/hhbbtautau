#!/bin/bash

# =================================================================
# Set up condor output directory for I/O to condor directory
# =================================================================
print_env_variable() { var="$1"; [ -z "${!var}" ] && echo "$var is not set" || echo "$var has been set to ${!var}"; }

export IS_CONDOR=true
print_env_variable "IS_CONDOR"

source lpcsetup.sh
export PREFIX=root://cmseos.fnal.gov
print_env_variable "PREFIX"

export CONDORPATH="root://cmseos.fnal.gov//store/user/joyzhou/output"
print_env_variable "CONDORPATH"

export SHORTPATH=/store/user/joyzhou/output
print_env_variable "SHORTPATH"

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

echo "CONDOR outputpath will be $DIRNAME"
