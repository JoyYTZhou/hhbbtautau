#!/bin/bash

# if not submitting batch jobs
if [ -z "${IS_CONDOR}" ]; then
    echo "Not submitting batch jobs"
    OUTPUTPATH="/uscms_data/d1/joyzhou/output"
    source scripts/venv.sh
# if submmitting batch jobs
else
    echo "submitting batch jobs"
    export OUTPUTPATH=$PWD/outputs
    # short path for executing eos commands locally
    export CONDORPATH="root://cmseos.fnal.gov//store/user/joyzhou/output"
fi

export SHORTPATH=/store/user/joyzhou/output
echo "shortname for condor output path is $SHORTPATH"
echo "Output directory is ${OUTPUTPATH}"

export ENV_FOR_DYNACONF=LPC
export VO_CMS_SW_DIR=/cvmfs/cms.cern.ch
source $VO_CMS_SW_DIR/cmsset_default.sh

source scripts/cleanpath.sh
