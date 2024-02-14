# ==========================================================================
# This script sets up condor environment and environment on any worker nodes
# Takes one argument: Process name
# ==========================================================================

export SUBMIT_DASK=true
echo "Currently in $PWD"
source $PWD/scripts/vomcheck.sh

print_env_variable() { var="$1"; [ -z "${!var}" ] && echo "$var is not set" || echo "$var has been set to ${!var}"; }

export IS_CONDOR=true
print_env_variable "IS_CONDOR"

export ENV_FOR_DYNACONF=LPC
export PREFIX=root://cmseos.fnal.gov
print_env_variable "PREFIX"

export CONDORPATH="root://cmseos.fnal.gov//store/user/joyzhou/output"
print_env_variable "CONDORPATH"

export SHORTPATH=/store/user/joyzhou/output
print_env_variable "SHORTPATH"

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

export OUTPUTPATH=$PWD/outputs
export HHBBTT=$PWD

if [ ! -z "$1" ]; then
    export OUTPUTPATH=$OUTPUTPATH/$1
    export PROCESS_NAME=$1
else
    export OUTPUTPATH=$OUTPUTPATH/all
    export PROCESS_NAME=all
fi
