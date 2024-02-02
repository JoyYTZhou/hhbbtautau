#!/bin/bash
# =================================================================
# This script sets up the environment to run this analysis in any execution machine
# Takes no argument
# =================================================================

export ENV_FOR_DYNACONF=LPC
export VO_CMS_SW_DIR=/cvmfs/cms.cern.ch
source $VO_CMS_SW_DIR/cmsset_default.sh

