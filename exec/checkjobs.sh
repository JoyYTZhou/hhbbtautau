#!/bin/bash

cd ..
source scripts/venv.sh
cd exec

export PYTHONPATH=$PWD/src:$PYTHONPATH

PROCESS_NAMES=("ZZ" "SingleH" "WW" "WZ" "WWZ" "WWW" "ggF" "DYJets" "TTbar" "ZH")

for PROCESS_NAME in "${PROCESS_NAMES[@]}"; do
    export PROCESS_NAME
    echo "Checking $PROCESS_NAME"
    python -c 'from analysis.spawndask import checkjobs; checkjobs()'
    echo "=============================================================="
done
    