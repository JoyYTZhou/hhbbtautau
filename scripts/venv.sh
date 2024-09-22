#!/bin/bash
# ===========================================================================================================
# This script sets up the environment to run this repository on lpc
# ===========================================================================================================

function display_help {
  echo "This script sets up the environment to run this repository on lpc"
  echo "Please run this script in the base level of the repository"
  echo "----------------------------------------------------------------------------------------------------"
  echo "source scripts/venv.sh [DYNA_ENVNAME]"
  echo "DYNA_ENVNAME: Dynaconf environment name"
  echo "If DYNA_ENVNAME is not provided, the script will prompt you to enter it"
  echo "Example: source scripts/venv.sh TEST"
  echo "This script will assume that you have a working python virtual environment called skim_el9."
  echo "If you do not have this virtual environment, please create it using the following commands:"
  echo "----------------------------------------------------------------------------------------------------"
  echo "source scripts/envsetup.sh"
  echo "Enter the name of the new virtual environment when prompted."
  echo "You may name the virtual environment anything you like, but if you name it something other than skim_el9, you will need to modify this script."
}

if [[ "$1" == "--help" || "$1" == "-h" ]]; then
  display_help
  return
fi

source scripts/envutil.sh
LCG_setup
LPC_setup
setup_dirname_local

PYTHON_ENV_NAME=skim_el9

export PYTHONPATH=~/nobackup/${PYTHON_ENV_NAME}/lib/python3.9/site-packages:$PYTHONPATH
source ~/nobackup/${PYTHON_ENV_NAME}/bin/activate
export PYTHONPATH=$PWD:$PYTHONPATH

export PATH=$(remove_duplicates "$PATH")
export PYTHONPATH=$(remove_duplicates "$PYTHONPATH")

echo "===================================="

if [ -z "$1" ]; then
  echo "Enter Dynaconf environment name: "
  read DYNA_ENVNAME
else
  DYNA_ENVNAME=$1
fi

export ENV_FOR_DYNACONF=$DYNA_ENVNAME
export DEBUG_MODE=false

alias jup='jupyter notebook --no-browser --port=2001'