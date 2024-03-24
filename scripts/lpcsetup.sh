#!/bin/bash
# ==================================================================================
# This script sets up the environment to run this analysis in any execution machine
# and gives convenient shortnames/alias
# Takes no argument
# ==================================================================================

export ENV_FOR_DYNACONF=LPC
export VO_CMS_SW_DIR=/cvmfs/cms.cern.ch
export EOS_MGM_URL=root://cmseos.fnal.gov
source $VO_CMS_SW_DIR/cmsset_default.sh

print_env_variable() { var="$1"; [ -z "${!var}" ] && echo "$var is not set" || echo "$var has been set to ${!var}"; }

alias lsoutput="eosls /store/user/joyzhou/output"

export PREFIX=root://cmseos.fnal.gov
print_env_variable "PREFIX"

export CONDORPATH="root://cmseos.fnal.gov//store/user/joyzhou/output"
print_env_variable "CONDORPATH"

export SHORTPATH=/store/user/joyzhou/output
print_env_variable "SHORTPATH"




