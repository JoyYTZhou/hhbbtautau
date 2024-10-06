#!/bin/bash

# This bash script is adapted from https://github.com/bu-cms/bucoffea/blob/master/bucoffea/execute/htcondor_wrap.sh

USERNAME=joyzhou
echo "Currently in $PWD"

export JSONPATH=$1
export ENV_FOR_DYNACONF=$2
export ENV_NAME=skim_el9

OLD_ENV_NAME=/uscms_data/d3/${USERNAME}/${ENV_NAME}

if [ ! -z "${VIRTUAL_ENV}" ] && [ "$VIRTUAL_ENV" == "${ENV_NAME}" ]; then
    echo "Found environmental variable."
else
    mkdir ${ENV_NAME}
    tar -xzf ${ENV_NAME}.tar.gz -C .
    sed -i "s|${OLD_ENV_NAME}|${PWD}/${ENV_NAME}|g" ${ENV_NAME}/bin/activate
    sed -i "s|${OLD_ENV_NAME}|${PWD}/${ENV_NAME}|g" ${ENV_NAME}/bin/*
    export VIRTUAL_ENV=${ENV_NAME}
fi

source scripts/envutil.sh
LCG_sasetup

export CONDOR_BASE=/store/user/${USERNAME}

source ${ENV_NAME}/bin/activate
set_python_path

export MPLCONFIGDIR=matplotlibconfig
export PYTHONPATH=$PWD:$PYTHONPATH
export PATH=$(remove_duplicates "$PATH")
export PYTHONPATH=$(remove_duplicates "$PYTHONPATH")

echo "This is the PYTHONPATH===================================="
echo $PYTHONPATH

export OUTPUT_BASE=$PWD
export DEBUG_MODE=true

echo "start executing main file"
checkproxy

LOG_FILE="debug_run.log"
GDB_BACKTRACE_FILE="gdb_backtrace.txt"
CORE_DUMP_FILE="core_dump"

command -v python >/dev/null 2>&1 || { echo "python is not installed. Exiting."; exit 1; }

echo "Running Python script with gdb for debugging..." | tee -a $LOG_FILE

gdb --batch --ex "run --input ${JSONPATH} --diagnose" --ex "bt" --args python -u main.py 2>&1 | tee $GDB_BACKTRACE_FILE

if [ -f "$CORE_DUMP_FILE" ]; then
    echo "Core dump found. Generating backtrace..." | tee -a $LOG_FILE
    gdb python "$CORE_DUMP_FILE" --batch -ex "bt" >> $GDB_BACKTRACE_FILE
else
    echo "No core dump found. Check gdb_backtrace.txt for details." | tee -a $LOG_FILE
fi

echo "Check $GDB_BACKTRACE_FILE for the backtrace and any relevant details." | tee -a $LOG_FILE
