#!/bin/bash
# ===========================================================================================================
# This script sets up a proper virtual environment to run this repository and create a tarball
# in the ~/nobackup area
# ===========================================================================================================

# NOTE: This script is meant for LCG set up with PYTHON<=3.10
cd ~/work/hhbbtautau
rm -r *.tar.gz
cd ~/nobackup

echo "=================================================================="
echo "Please enter the name of the new virtual environment: "
read ENV_NAME
echo "=================================================================="

rm ${ENV_NAME}.tar.gz
rm -r ${ENV_NAME}

python -m venv ${ENV_NAME}
echo "creating new venv..."
source ${ENV_NAME}/bin/activate

python -m pip install coffea --upgrade --no-cache-dir
echo "Installed coffea"
python -m pip install dynaconf --no-cache-dir
echo "Installed dyanconf"
python -m pip install htcondor --no-cache-dir
echo "Installed htcondor"
python -m pip install rucio-clients==32 --no-cache-dir
deactivate

# create tarball
tar -czf ~/work/hhbbtautau/${ENV_NAME}.tar.gz ${ENV_NAME}
cd ~/work/hhbbtautau
