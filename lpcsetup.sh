#!/bin/bash

# if not submitting batch jobs
if [ -z "${IS_CONDOR}" ]; then
    OUTPUTPATH="/uscms_data/d1/joyzhou/output"
    export PATH=$PATH:/uscms/home/joyzhou/.local/bin
    source /uscms_data/d1/joyzhou/miniconda3/etc/profile.d/conda.sh
# if submmitting batch jobs
else
    export OUTPUTPATH=$PWD/outputs
    # short path for executing eos commands locally
    export SHORTPATH=/store/user/joyzhou/output
    export CONDORPATH="root://cmseos.fnal.gov//store/user/joyzhou/output"
fi

echo "Output directory is ${OUTPUTPATH}"

export ENV_FOR_DYNACONF=LPC
export VO_CMS_SW_DIR=/cvmfs/cms.cern.ch
source $VO_CMS_SW_DIR/cmsset_default.sh
